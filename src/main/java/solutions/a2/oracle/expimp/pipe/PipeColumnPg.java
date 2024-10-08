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
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleTypes;
import oracle.sql.DATE;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.NUMBER;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

/**
 * 
 * Column definition for pipe table
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class PipeColumnPg extends PipeColumn implements PipeColumnBind {

	public PipeColumnPg(final OracleResultSet oraResultSet) throws SQLException {
		super(oraResultSet);
	}

	public String columnName() {
		return columnName;
	}

	@Override
	public void bindData(int index, OracleResultSet resultSet, PreparedStatement insertData) throws SQLException {
		switch (jdbcType) {
		case OracleTypes.TIMESTAMPLTZ:
			final TIMESTAMPLTZ tsLtz = resultSet.getTIMESTAMPLTZ(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.TIMESTAMP);
			else
				insertData.setTimestamp(index, tsLtz.timestampValue());
			break;
		case OracleTypes.TIMESTAMPTZ:
			final TIMESTAMPTZ tsTz = resultSet.getTIMESTAMPTZ(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
			else
				insertData.setObject(index, tsTz.offsetDateTimeValue());
			break;
		case OracleTypes.TIMESTAMP:
			final TIMESTAMP ts = resultSet.getTIMESTAMP(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.TIMESTAMP);
			else
				insertData.setTimestamp(index, ts.timestampValue());
			break;
		case OracleTypes.DATE:
			final DATE date = resultSet.getDATE(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.TIMESTAMP);
			else
				insertData.setTimestamp(index, date.timestampValue());
			break;
		case OracleTypes.INTERVALYM:
			//TODO
			final INTERVALYM iym = resultSet.getINTERVALYM(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setINTERVALYM(index, iym);
			break;
		case OracleTypes.INTERVALDS:
			//TODO
			final INTERVALDS ids = resultSet.getINTERVALDS(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setINTERVALDS(index, ids);
			break;
		case OracleTypes.TINYINT:
			final byte byteValue = resultSet.getByte(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.TINYINT);
			else
				insertData.setByte(index, byteValue);
			break;
		case OracleTypes.SMALLINT:
			final short shortValue = resultSet.getShort(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.SMALLINT);
			else
				insertData.setShort(index, shortValue);
			break;
		case OracleTypes.INTEGER:
			final int intValue = resultSet.getInt(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.INTEGER);
			else
				insertData.setInt(index, intValue);
			break;
		case OracleTypes.BIGINT:
			final long longValue = resultSet.getLong(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.BIGINT);
			else
				insertData.setLong(index, longValue);
			break;
		case OracleTypes.NUMBER:
			final NUMBER number = resultSet.getNUMBER(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.NUMERIC);
			else
				insertData.setBigDecimal(index, number.bigDecimalValue());
			break;
		case OracleTypes.BINARY_FLOAT:
			final float fNumber = resultSet.getFloat(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.FLOAT);
			else
				insertData.setFloat(index, fNumber);
			break;
		case OracleTypes.BINARY_DOUBLE:
			final double dNumber = resultSet.getDouble(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.DOUBLE);
			else
				insertData.setDouble(index, dNumber);
			break;
		case OracleTypes.CHAR:
		case OracleTypes.VARCHAR:
			final String string = resultSet.getString(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.VARCHAR);
			else
				insertData.setString(index, StringUtils.replace((String) string, "\0", StringUtils.EMPTY));
			break;
		case OracleTypes.NCHAR:
		case OracleTypes.NVARCHAR:
			final String nString = resultSet.getNString(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.NVARCHAR);
			else
				insertData.setNString(index, StringUtils.replace((String) nString, "\0", StringUtils.EMPTY));
			break;
		case OracleTypes.CLOB:
			final Clob clob = resultSet.getClob(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.CLOB);
			else
				try (Reader reader = clob.getCharacterStream()) {
					insertData.setClob(index, reader);
				} catch (IOException ioe) {
					throw new SQLException(ioe);
				}
			break;
		case OracleTypes.NCLOB:
			final NClob nClob = resultSet.getNClob(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.NCLOB);
			else
				try (Reader reader = nClob.getCharacterStream()) {
					insertData.setClob(index, reader);
				} catch (IOException ioe) {
					throw new SQLException(ioe);
				}
			break;
		case OracleTypes.BINARY:
			final byte[] ba = resultSet.getBytes(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.BINARY);
			else
				insertData.setBytes(index, ba);
			break;
		case OracleTypes.BLOB:
			final Blob blob = resultSet.getBlob(index);
			if (resultSet.wasNull())
				insertData.setNull(index, Types.BLOB);
			else
				try (InputStream stream = blob.getBinaryStream()) {
					insertData.setBlob(index, stream);
				} catch (IOException ioe) {
					throw new SQLException(ioe);
				}
			break;
		case OracleTypes.SQLXML:
			final SQLXML sqlxml = resultSet.getSQLXML(index);
			if (resultSet.wasNull())
				insertData.setSQLXML(index, sqlxml);
			else
				try (InputStream stream = sqlxml.getBinaryStream()) {
					insertData.setBinaryStream(index, stream);
				} catch (IOException ioe) {
					throw new SQLException(ioe);
				}
			break;
		}
	}

}
