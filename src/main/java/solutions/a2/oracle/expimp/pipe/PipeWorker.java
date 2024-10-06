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

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;

/**
 * 
 * Pipe worker
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class PipeWorker extends Thread {

	private static final Logger LOGGER = LoggerFactory.getLogger(PipeWorker.class);
	private static final int PRINT_TO_LOG = 1000;

	private final CountDownLatch latch;
	private final int rowNumStart;
	private final int rowNumEnd;
	private final PipeTable table;
	private final OracleConnection connSource;
	private final OracleConnection connDest;
	private final int commitAfter;
	private final boolean useDefaultFetchSize;

	private static final int MAX_ROWS = 32767;
	private static final int ORA_1461 = 1461;

	public PipeWorker(
			final int workerNum,
			final CountDownLatch latch,
			final int rowNumStart,
			final int rowNumEnd,
			final PipeTable table,
			final OracleConnection connSource,
			final OracleConnection connDest,
			final int commitAfter,
			final boolean useDefaultFetchSize) throws SQLException {
		this.setDaemon(true);
		this.setName("pipe-" + workerNum);
		this.latch = latch;
		this.rowNumStart = rowNumStart;
		this.rowNumEnd = rowNumEnd;
		this.table = table;
		this.connSource = connSource;
		this.connDest = connDest;
		this.commitAfter = commitAfter;
		this.useDefaultFetchSize = useDefaultFetchSize;
	}

	@Override
	public void run() {
		int currentRowIndex = rowNumStart;
		int rowsToCommit = 0;
		try {
			long elapsed = System.currentTimeMillis();
			int rowsProcessed = 0;
			final OracleCallableStatement selectData = table.prepareSource(connSource);
			final PreparedStatement insertData = table.prepareDest(connDest);
			if (!useDefaultFetchSize) {
				selectData.setFetchSize(rowNumEnd - rowNumStart);
			}
			final int batchCount = (int) Math.ceil( ((double)(rowNumEnd - rowNumStart))/MAX_ROWS);
			LOGGER.info(
					"\n" +
					"=====================\n" +
					"Thread {}: will process rows from {} to {} in {} batche(s).\n" +
					"=====================\n",
					this.getName(), rowNumStart, rowNumEnd - 1, batchCount);
			for (int batchNum = 0; batchNum < batchCount; batchNum++) {
				long elapsedBatch = System.currentTimeMillis();
				final int batchStart;
				final int batchEnd;
				if (batchCount == 1) {
					batchStart = rowNumStart;
					batchEnd = rowNumEnd;
				} else {
					batchStart = rowNumStart + (MAX_ROWS * batchNum);
					if (batchNum == batchCount - 1) {
						batchEnd = rowNumEnd;
					} else {
						batchEnd = batchStart + MAX_ROWS;
					}
				}
				final Array rowIdArray = table.getRowIdArray(connSource, batchStart, batchEnd);
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"Thread {}, Batch {}: will process rows from {} to {}\n" +
						"Prepared to run in {} milliseconds\n" +
						"=====================\n",
						this.getName(), batchNum, batchStart, batchEnd - 1, System.currentTimeMillis() - elapsedBatch);
				elapsedBatch = System.currentTimeMillis();
				long elapsed4Part = elapsedBatch;

				rowsToCommit = 0;
				int rowsProcessedInBatch = 0;
				int rowsToPrintLog = 0;
				selectData.registerOutParameter(1, OracleTypes.CURSOR);
				selectData.setArray(2, rowIdArray);
				selectData.execute();
				final OracleResultSet resultSet = (OracleResultSet) selectData.getCursor(1);
				currentRowIndex = batchStart;
				while (resultSet.next()) {
					table.processRow(resultSet, insertData);
					insertData.addBatch();
					rowsProcessed++;
					rowsProcessedInBatch++;
					rowsToCommit++;
					rowsToPrintLog++;
					if (rowsToCommit == commitAfter) {
						insertData.executeBatch();
						connDest.commit();
						rowsToCommit = 0;
					}
					if (rowsToPrintLog == PRINT_TO_LOG) {
						final long currentTime = System.currentTimeMillis();
						LOGGER.info("{} rows processed in {} ms.",
								PRINT_TO_LOG, currentTime - elapsed4Part);
						elapsed4Part = currentTime;
						rowsToPrintLog = 0;
					}
					currentRowIndex++;
				}
				if (rowsToCommit > 0) {
					insertData.executeBatch();
					connDest.commit();
				}
				resultSet.close();
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"Thread {}, Batch {}: {} rows processed in {} milliseconds\n" +
						"=====================\n",
						this.getName(), batchNum, rowsProcessedInBatch, System.currentTimeMillis() - elapsedBatch);
			}
			selectData.close();
			insertData.close();
			connSource.close();
			connDest.close();
			LOGGER.info(
					"\n" +
					"=====================\n" +
					"Thread {}: {} rows processed in {} milliseconds\n" +
					"=====================\n",
					this.getName(), rowsProcessed, System.currentTimeMillis() - elapsed);
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == ORA_1461) {
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"ORA-01461: Please add to destination URL parameter 'defaultNChar=true' and restart process!\n" +
						"=====================\n");
			} else {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			}
		}
		latch.countDown();
	}

}

