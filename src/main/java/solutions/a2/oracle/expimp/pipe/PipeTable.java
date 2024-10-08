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

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;

/**
 * 
 * Table definition
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class PipeTable {

	private static final Logger LOGGER = LoggerFactory.getLogger(PipeTable.class);

	private final String sourceTableOwner;
	private final String sourceTableName;
	private final String destinationTableOwner;
	private final String destinationTableName;
	private final List<PipeColumn> allColumns;
	private String sqlSelectKeys, sqlSelectData, sqlInsertData;
	private final RowIdStore rowIdStore;
	private final boolean addRowId;

	public PipeTable(
			final OracleConnection connection,
			final String sourceTableOwner,
			final String sourceTableName,
			final int type,
			final String destinationTableOwner,
			final String destinationTableName,
			final String whereClause,
			final int rowIdStoreType,
			final String rowIdColumnName) throws SQLException, IOException {
		this.sourceTableOwner = sourceTableOwner;
		this.sourceTableName = sourceTableName;
		this.destinationTableOwner = destinationTableOwner;
		this.destinationTableName = destinationTableName;
		this.allColumns = new ArrayList<>();
		if (StringUtils.isNotBlank(rowIdColumnName)) {
			addRowId = true;
		} else {
			addRowId = false;
		}
		if (rowIdStoreType == ExpImpPipe.ROWID_STORE_CQ) {
			this.rowIdStore = new RowIdStoreChronicleQueue(sourceTableOwner, sourceTableName);
		} else {
			//ROWID_STORE_LIST
			this.rowIdStore = new RowIdStoreArrayList();
		}
		fillColumnInfo(connection, whereClause, rowIdColumnName, type);

		long elapsed = System.currentTimeMillis();
		rowIdStore.readKeys(connection, sqlSelectKeys);
		elapsed = System.currentTimeMillis() - elapsed;
		LOGGER.info(
				"\n" +
				"=====================\n" +
				"{}.{} :\n" +
				"\t{} rows read in {} milliseconds\n" +
				"=====================\n",
				sourceTableOwner, sourceTableName, rowIdStore.size(), elapsed);
	}

	protected int rowCount()  {
		return rowIdStore.size();
	}

	protected Array getRowIdArray(final OracleConnection connSource, final int rowNumStart, final int rowNumEnd) throws SQLException {
		return rowIdStore.getRowIdArray(connSource, rowNumStart, rowNumEnd);
	}

	protected OracleCallableStatement prepareSource(
			final OracleConnection connSource) throws SQLException {
		return (OracleCallableStatement) connSource.prepareCall(sqlSelectData);
	}

	protected PreparedStatement prepareDest(
			final Connection connDest) throws SQLException {
		return connDest.prepareStatement(sqlInsertData);
	}

	protected void processRow(
			final OracleResultSet resultSet,
			final PreparedStatement insertData) throws SQLException {
		final int diff;
		if (addRowId) {
			diff = 2;
			final RowId rowId = resultSet.getRowId(1);
			if (resultSet.wasNull())
				insertData.setNull(1, OracleTypes.VARCHAR);
			else
				insertData.setString(1, rowId.toString());
		} else {
			diff = 1;
		}
		for (int i = 0; i < allColumns.size(); i++) {
				final PipeColumnBind column = (PipeColumnBind) allColumns.get(i);
				column.bindData(i + diff, resultSet, insertData);
		}
	}

	private void fillColumnInfo(
			final OracleConnection connection,
			final String whereClause,
			final String rowIdColumnName,
			final int type) throws SQLException {
		/*
select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION,
       C.DATA_SCALE, C.NULLABLE, C.COLUMN_ID, C.DATA_DEFAULT, L.CHUNK
from   DBA_TAB_COLS C 
left join DBA_LOBS L on C.OWNER=L.OWNER and C.TABLE_NAME=L.TABLE_NAME and C.COLUMN_NAME=L.COLUMN_NAME
where  C.HIDDEN_COLUMN='NO'
  and  C.OWNER='SCOTT' and C.TABLE_NAME='DEPT'
  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB', 'NCLOB')
       or C.DATA_TYPE like 'TIMESTAMP%' or C.DATA_TYPE like 'INTERVAL%'
       or (C.DATA_TYPE='XMLTYPE' and C.DATA_TYPE_OWNER in ('SYS','PUBLIC')))
order by C.COLUMN_ID;
		 */
		try {
			final PreparedStatement statement = connection.prepareStatement(
					"select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION,\n" +
					"       C.DATA_SCALE, C.NULLABLE, C.COLUMN_ID, C.DATA_DEFAULT, L.CHUNK\n" +
					"from   DBA_TAB_COLS C \n" +
					"left join DBA_LOBS L on C.OWNER=L.OWNER and C.TABLE_NAME=L.TABLE_NAME and C.COLUMN_NAME=L.COLUMN_NAME\n" +
					"where  C.HIDDEN_COLUMN='NO'\n" +
					"  and  C.OWNER=? and C.TABLE_NAME=?\n" +
					"  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB', 'NCLOB')\n" +
					"       or C.DATA_TYPE like 'TIMESTAMP%' or C.DATA_TYPE like 'INTERVAL%'\n" +
					"       or (C.DATA_TYPE='XMLTYPE' and C.DATA_TYPE_OWNER in ('SYS','PUBLIC')))\n" +
					"order by C.COLUMN_ID",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, sourceTableOwner);
			statement.setString(2, sourceTableName);
			final OracleResultSet oraResultSet = (OracleResultSet) statement.executeQuery();

			boolean firstValue = true;
			final StringBuilder selectKeys = new StringBuilder(256);
			selectKeys
				.append("select ROWID from ")
				.append(sourceTableOwner)
				.append(".")
				.append(sourceTableName)
				.append(" KU$");
			final StringBuilder selectData = new StringBuilder(2048);
			selectData
				.append("begin\n")
				.append("  open ? for\n")
				.append("  select ");
			final StringBuilder insertData = new StringBuilder(2048);
			insertData
					.append("insert into ")
					.append(destinationTableOwner)
					.append(".")
					.append(destinationTableName)
					.append("(");
			if (addRowId) {
				selectData.append("KU$.rowid");
				insertData
					.append(rowIdColumnName);
				firstValue = false;
			}
			while (oraResultSet.next()) {
				final PipeColumn pipeColumn;
				if (type == PipePool.TYPE_ORA) {
					pipeColumn = new PipeColumnOra(oraResultSet);
				} else {
					pipeColumn = new PipeColumnPg(oraResultSet);
				}
				allColumns.add(pipeColumn);
				if (firstValue) {
					firstValue = false;
				} else {
					selectData.append(", ");
					insertData.append(", ");
				}
				selectData.append(pipeColumn.columnName());
				insertData.append(pipeColumn.columnName());
			}

			if (StringUtils.isNotBlank(whereClause)) {
				selectKeys
					.append("\n")
					.append(whereClause);
			}

			selectData
				.append("\n")
				.append("  from ")
				.append(sourceTableOwner)
				.append(".")
				.append(sourceTableName)
				.append(" KU$, (select * from table(?)) R\n")
				.append("  where KU$.rowid=R.COLUMN_VALUE;")
				.append("end;");

			insertData.append(")\nvalues(");
			firstValue = true;
			if (addRowId) {
				insertData.append("?");
				firstValue = false;
			}
			for (int i = 0; i < allColumns.size(); i++) {
				if (firstValue) {
					firstValue = false;
				} else {
					insertData.append(", ");
				}
				insertData.append("?");
			}
			insertData.append(")");

			sqlSelectKeys = selectKeys.toString();
			sqlSelectData = selectData.toString();
			sqlInsertData = insertData.toString();
			LOGGER.info(
					"\n" +
					"=====================\n" +
					"{}\n" +
					"\t-- will be used for select key(s) data\n\n" + 
					"{}\n" +
					"\t-- will be used for select source data\n\n" + 
					"{}\n" +
					"\t-- will be used for insert data to destination\n" + 
					"=====================\n",
					sqlSelectKeys, sqlSelectData, sqlInsertData);

		} catch(SQLException sqle) {
			if (sqle.getErrorCode() == 942) {
				// ORA-00942: table or view does not exist
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"Please run as SYSDBA:\n" +
						"\tgrant select on DBA_TAB_COLS to {};\n" + 
						"\tgrant select on DBA_LOBS to {};\n" +
						"And restart pipe utility\n" +
						"=====================\n",
						connection.getUserName(), connection.getUserName());
			}
			throw sqle;
		}
	}

	protected void close() {
		rowIdStore.close();
	}

}
