/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.oracle.expimp.pipe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ValueIn;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Chronicle Queue implementation of RowId store
 * When using this implementation do not forget to set XX:MaxDirectMemorySize 
 * to appropriate value!
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class RowIdStoreChronicleQueue implements RowIdStore {

	private static final Logger LOGGER = LoggerFactory.getLogger(RowIdStoreChronicleQueue.class);

	private final Path queueDirectory;
	private final ChronicleQueue rowIdQueue;
	private int queueSize;

	RowIdStoreChronicleQueue(
			final String sourceTableOwner,
			final String sourceTableName) throws IOException {
		//TODO - add directory to create as parameter
		this.queueDirectory = Files.createTempDirectory(
				Paths.get(System.getProperty("java.io.tmpdir")),
				sourceTableOwner + "_" + sourceTableName + ".");
		this.rowIdQueue = ChronicleQueue
				.singleBuilder(queueDirectory)
				.build();
		queueSize = 0;
	}

	@Override
	public void readKeys(final OracleConnection connection,
			final String sqlSelectKeys) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sqlSelectKeys,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		OracleResultSet resultSet = (OracleResultSet) statement.executeQuery();
		final ExcerptAppender appender = rowIdQueue.acquireAppender();
		while (resultSet.next()) {
			final String rowId = resultSet.getROWID(1).stringValue();
			appender.writeDocument(w -> w.getValueOut().text(rowId));
			queueSize++;
		}
		appender.close();
		resultSet.close();
		resultSet = null;
		statement.close();
		statement = null;
	}

	@Override
	public int size() {
		return queueSize;
	}

	@Override
	public Array getRowIdArray(OracleConnection connection, int rowNumStart, int rowNumEnd) throws SQLException {
		final ExcerptTailer tailer = rowIdQueue.createTailer();
		tailer.moveToIndex(tailer.index() + rowNumStart);
		final int arraySize = rowNumEnd - rowNumStart;
		final String[] rowIds = new String[arraySize];
		int i = 0;
		for (int rowNum = rowNumStart; rowNum < rowNumEnd; rowNum++) {
			final StringBuilder sb = new StringBuilder();
			tailer.readDocument(w -> {
				ValueIn in = w.getValueIn();
				sb.append(in.text());
			});
			rowIds[i++] = sb.toString();
		}
		tailer.close();
		return connection.createOracleArray("SYS.ODCIVARCHAR2LIST", rowIds);
	}

	@Override
	public void close() {
		rowIdQueue.close();
		try {
			Files.walk(queueDirectory)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
		} catch (IOException ioe) {
			LOGGER.error(
					"\n" +
					"=====================\n" +
					"Unable to delete Chronicle Queue files in '{}' due to '{}' error!\n" +
					"=====================\n",
					queueDirectory, ioe.getMessage());
		}
	}

}
