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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;

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
public class PipeColumnOra extends PipeColumn implements PipeColumnBind {

	public PipeColumnOra(final ResultSet resultSet) throws SQLException {
		super(resultSet);
	}

	@Override
	public String columnName() {
		return columnName;
	}

	@Override
	public void bindData(int index, OracleResultSet resultSet, PreparedStatement insertData) throws SQLException {
		switch (jdbcType) {
		case OracleTypes.TIMESTAMPLTZ:
			final TIMESTAMPLTZ tsLtz = resultSet.getTIMESTAMPLTZ(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setTIMESTAMPLTZ(index, tsLtz);
			break;
		case OracleTypes.TIMESTAMPTZ:
			final TIMESTAMPTZ tsTz = resultSet.getTIMESTAMPTZ(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setTIMESTAMPTZ(index, tsTz);
			break;
		case OracleTypes.TIMESTAMP:
			final TIMESTAMP ts = resultSet.getTIMESTAMP(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setTIMESTAMP(index, ts);
			break;
		case OracleTypes.INTERVALYM:
			final INTERVALYM iym = resultSet.getINTERVALYM(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setINTERVALYM(index, iym);
			break;
		case OracleTypes.INTERVALDS:
			final INTERVALDS ids = resultSet.getINTERVALDS(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setINTERVALDS(index, ids);
			break;
		case OracleTypes.DATE:
			final DATE date = resultSet.getDATE(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setDATE(index, date);
			break;
		case OracleTypes.NUMBER:
			final NUMBER number = resultSet.getNUMBER(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				((OraclePreparedStatement)insertData).setNUMBER(index, number);
			break;
		case OracleTypes.BINARY_FLOAT:
			final float fNumber = resultSet.getFloat(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				insertData.setFloat(index, fNumber);
			break;
		case OracleTypes.BINARY_DOUBLE:
			final double dNumber = resultSet.getDouble(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				insertData.setDouble(index, dNumber);
			break;
		case OracleTypes.CHAR:
		case OracleTypes.VARCHAR:
			final String string = resultSet.getString(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				insertData.setString(index, string);
			break;
		case OracleTypes.NCHAR:
		case OracleTypes.NVARCHAR:
			final String nString = resultSet.getNString(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
			else
				insertData.setNString(index, nString);
			break;
		case OracleTypes.CLOB:
			final Clob clob = resultSet.getClob(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
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
				insertData.setNull(index, jdbcType);
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
				insertData.setNull(index, jdbcType);
			else
				insertData.setBytes(index, ba);
			break;
		case OracleTypes.BLOB:
			final Blob blob = resultSet.getBlob(index);
			if (resultSet.wasNull())
				insertData.setNull(index, jdbcType);
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
