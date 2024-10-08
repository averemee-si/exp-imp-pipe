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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

/**
 * 
 * PipeConnection: OracleUCP pool container
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class PipePool {

	private static final Logger LOGGER = LoggerFactory.getLogger(PipePool.class);
	private static final int DEFAULT_SDU = 8192;

	private static final String DRIVER_ORACLE = "oracle.jdbc.pool.OracleDataSource";
	private static final String PREFIX_ORACLE = "jdbc:oracle:";
	private static final String DRIVER_POSTGRESQL = "org.postgresql.Driver";
	private static final String PREFIX_POSTGRESQL = "jdbc:postgresql:";

	static int TYPE_ORA = 0;
	static int TYPE_PG = 1;

	private final PoolDataSource pds;
	private final String poolName;
	private final int type;
	private int version = 0;
	private int dbCores = 1;

	private PipePool(final String poolName, final String dbUrl) throws SQLException {
		this.poolName = poolName;
		pds = PoolDataSourceFactory.getPoolDataSource();
		if (StringUtils.startsWith(dbUrl, PREFIX_ORACLE)) {
			pds.setConnectionFactoryClassName(DRIVER_ORACLE);
			type = TYPE_ORA;
		} else if (StringUtils.startsWith(dbUrl, PREFIX_POSTGRESQL)) {
			pds.setConnectionFactoryClassName(DRIVER_POSTGRESQL);
			type = TYPE_PG;
		} else {
			throw new SQLException("Unsupported JDBC URL " + dbUrl + " !");
		}
		pds.setConnectionPoolName(poolName);
		pds.setURL(dbUrl);
	}

	public static PipePool get(final String poolName,
			final String dbUrl, final String dbUser, final String dbPassword)
					throws SQLException {
		PipePool oco = new PipePool(poolName, dbUrl);
		oco.setUserPassword(dbUser, dbPassword);

		if (oco.type == TYPE_ORA) {
			OracleConnection oc = (OracleConnection) oco.pds.getConnection();
			final int sdu = ((oracle.jdbc.internal.OracleConnection) oc).getNegotiatedSDU();
			try (PreparedStatement ps = oc.prepareStatement(
					"select VERSION, INSTANCE_NUMBER, INSTANCE_NAME, HOST_NAME, THREAD#,\n" +
					"(select nvl(CPU_CORE_COUNT_CURRENT, CPU_COUNT_CURRENT) from V$LICENSE) CPU_CORE_COUNT_CURRENT\n" +
					"from   V$INSTANCE");
				ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					//TODO
					//TODO Print connection information
					//TODO
					oco.dbCores = rs.getInt("CPU_CORE_COUNT_CURRENT");
				}
			} catch (SQLException sqle) {
				if (sqle.getErrorCode() == 942) {
					// ORA-00942: table or view does not exist
					LOGGER.error(
							"\n" +
							"=====================\n" +
							"Please run as SYSDBA:\n" +
							"\tgrant select on V_$INSTANCE to {};\n" + 
							"\tgrant select on V_$LICENSE to {};\n" +
							"And restart {}\n" +
							"=====================\n",
							oc.getUserName(), oc.getUserName(), ExpImpPipe.class.getName());				
				}
				throw sqle;
			}
			oc.close();
			if (sdu <= DEFAULT_SDU) {
				LOGGER.warn(
						"\n" +
						"=====================\n" +
						"The negotiated SDU for connection to '{}' is set to {}.\n" +
						"\tWe recommend increasing it to achieve better performance.\n" + 
						"\tInstructions on how to do this can be found at\n" +
						"\t\t\thttps://github.com/averemee-si/oracdc#performance-tips\n" +
						"=====================\n",
						dbUrl, sdu);
			}
		} else {
			// oco.type == TYPE_PG
			Connection oc = oco.pds.getConnection();
			try (PreparedStatement ps = oc.prepareStatement(
					"SELECT setting FROM pg_settings WHERE name='max_worker_processes'");
				ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					oco.dbCores = rs.getInt("setting");
				}
				
			} catch (SQLException sqle) {
				
			}
		}
		return oco;
	}



	private void setUserPassword(final String dbUser, final String dbPassword) throws SQLException {
		pds.setUser(dbUser);
		pds.setPassword(dbPassword);
	}

	public Connection getConnection() throws SQLException {
		if (type == TYPE_ORA) {
			try {
				Connection connection = pds.getConnection();
				connection.setClientInfo("OCSID.MODULE", "exp-imp-pipe");
				connection.setClientInfo("OCSID.CLIENTID", poolName);
				connection.setAutoCommit(false);
				return connection;
			} catch(SQLException sqle) {
				if (sqle.getCause() instanceof UniversalConnectionPoolException) {
					UniversalConnectionPoolException ucpe = (UniversalConnectionPoolException) sqle.getCause();
					// Try to handle UCP-45003 and UCP-45386
					// Ref.: https://docs.oracle.com/en/database/oracle/oracle-database/21/jjucp/error-codes-reference.html
					if (ucpe.getErrorCode() == 45003 || ucpe.getErrorCode() == 45386) {
						LOGGER.error("Trying to handle UCP-{} with error message:\n{}",
								ucpe.getErrorCode(), ucpe.getMessage());
						final String newPoolName = poolName + "-" + (version++);
						LOGGER.error("Renaming pool '{}' to '{}'",
								pds.getConnectionPoolName(), newPoolName);
						try {
							Thread.sleep(5);
						} catch (InterruptedException ie) {}
						pds.setConnectionPoolName(newPoolName);
						return getConnection();
					} else {
						throw sqle;
					}
				} else {
					throw sqle;
				}
			}
		} else {
			// TYPE_PG
			final Connection connection = pds.getConnection();
			connection.setAutoCommit(false);
			return connection;
		}
	}

	public int getDbCoreCount() {
		return dbCores;
	}

	public void setPoolSize(final int degree) throws SQLException {
		pds.setMinPoolSize(degree);
		pds.setMaxPoolSize(degree + 8);
	}

	public void destroy() throws SQLException {
		try {
			UniversalConnectionPoolManager mgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
			mgr.destroyConnectionPool(poolName);
		} catch (UniversalConnectionPoolException ucpe) {
			throw new SQLException(ucpe);
		}
	}

	public int getType() {
		return type;
	}

}
