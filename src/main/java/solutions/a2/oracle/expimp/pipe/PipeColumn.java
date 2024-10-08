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

import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;
import oracle.sql.NUMBER;

/**
 * 
 * Column definition for pipe table
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class PipeColumn {

	private static final Logger LOGGER = LoggerFactory.getLogger(PipeColumn.class);

	final String columnName;
	int chunk;
	int jdbcType;

	PipeColumn(final String columnName, final String oraType, final int chunk, final NUMBER precision, final NUMBER scale) throws SQLException {
		this.columnName = columnName;
		this.chunk = chunk;
		if (StringUtils.startsWith(oraType, "TIMESTAMP")) {
			if (StringUtils.endsWith(oraType, "WITH LOCAL TIME ZONE")) {
				jdbcType = OracleTypes.TIMESTAMPLTZ;
			} else if (StringUtils.endsWith(oraType, "WITH TIME ZONE")) {
				jdbcType = OracleTypes.TIMESTAMPTZ;
			} else {
				jdbcType = OracleTypes.TIMESTAMP;
			}			
		} else if (StringUtils.startsWith(oraType, "INTERVAL")) {
			if (StringUtils.contains(oraType, "TO MONTH")) {
				jdbcType = OracleTypes.INTERVALYM;
			} else {
				jdbcType = OracleTypes.INTERVALDS;
			}
		} else {
			switch (oraType) {
			case "DATE":
				jdbcType = OracleTypes.DATE;
				break;
			case "FLOAT":
			case "NUMBER":
				if (precision == null && scale == null) {
					jdbcType = OracleTypes.NUMBER;
				} else if (scale.isZero() && precision != null) {
					if (precision.shortValue() < 3) {
						jdbcType = OracleTypes.TINYINT;
					} else if (precision.shortValue() < 5) {
						jdbcType = OracleTypes.SMALLINT;
					} else if (precision.shortValue() < 10) {
						jdbcType = OracleTypes.INTEGER;
					} else if (precision.shortValue() < 19) {
						jdbcType = OracleTypes.BIGINT;
					} else {
						jdbcType = OracleTypes.NUMBER;
					}
				} else {
					jdbcType = OracleTypes.NUMBER;
				}
				break;
			case "BINARY_FLOAT":
				jdbcType = OracleTypes.BINARY_FLOAT;
				break;
			case "BINARY_DOUBLE":
				jdbcType = OracleTypes.BINARY_DOUBLE;
				break;
			case "CHAR":
				jdbcType = OracleTypes.CHAR;
				break;
			case "NCHAR":
				jdbcType = OracleTypes.NCHAR;
				break;
			case "VARCHAR2":
				jdbcType = OracleTypes.VARCHAR;
				break;
			case "NVARCHAR2":
				jdbcType = OracleTypes.NVARCHAR;
				break;
			case "ROWID":
				jdbcType = OracleTypes.ROWID;
				break;
			case "CLOB":
				jdbcType = OracleTypes.CLOB;
				break;
			case "NCLOB":
				jdbcType = OracleTypes.NCLOB;
				break;
			case "RAW":
				jdbcType = OracleTypes.BINARY;
				break;
			case "BLOB":
				jdbcType = OracleTypes.BLOB;
				break;
			case "XMLTYPE":
				jdbcType = OracleTypes.SQLXML;
				break;
			default:
				LOGGER.warn(
						"\n" +
						"=====================\n" +
						"Datatype '{}' for Column '{}' is not supported\n" +
						"\nType is set to JAVA_OBJECT!\n" + 
						"=====================\n",
						oraType, columnName);
				jdbcType = OracleTypes.JAVA_OBJECT;
			}
		}

	}

	PipeColumn(final OracleResultSet oraResultSet) throws SQLException {
		this(oraResultSet.getString("COLUMN_NAME"),
				oraResultSet.getString("DATA_TYPE"),
				oraResultSet.getInt("CHUNK"),
				oraResultSet.getNUMBER("DATA_PRECISION"),
				oraResultSet.getNUMBER("DATA_SCALE"));
	}

	public String columnName() {
		return columnName;
	}

}
