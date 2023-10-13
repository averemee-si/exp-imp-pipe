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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;
import oracle.sql.ROWID;

/**
 * 
 * Table definition
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class RowIdStoreArrayList implements RowIdStore {

	private final List<ROWID> keyArray;
	
	RowIdStoreArrayList() {
		this.keyArray = new ArrayList<>();
	}

	@Override
	public void readKeys(final OracleConnection connection,
			final String sqlSelectKeys) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sqlSelectKeys,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		OracleResultSet resultSet = (OracleResultSet) statement.executeQuery();
		while (resultSet.next()) {
			keyArray.add(resultSet.getROWID(1));
		}
		resultSet.close();
		resultSet = null;
		statement.close();
		statement = null;
	}

	@Override
	public int size() {
		return keyArray.size();
	}

	@Override
	public Array getRowIdArray(OracleConnection connection, int rowNumStart, int rowNumEnd) throws SQLException {
		final int arraySize = rowNumEnd - rowNumStart;
		final String[] rowIds = new String[arraySize];
		int i = 0;
		for (int rowNum = rowNumStart; rowNum < rowNumEnd; rowNum++) {
			rowIds[i++] = keyArray.get(rowNum).stringValue();
		}
		return connection.createOracleArray("SYS.ODCIVARCHAR2LIST", rowIds);
	}

	@Override
	public void close() {
	}

}
