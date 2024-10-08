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
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;

/**
 * 
 * ExpImpPipe entry point
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class ExpImpPipe {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExpImpPipe.class);
	private static final int ROWS_TO_COMMIT = 50;
	private static final String ROWID_KEY = "ORA_ROW_ID";

	protected static int ROWID_STORE_LIST = 1;
	protected static int ROWID_STORE_CQ = 2;

	public static void main(String[] argv) {
		LOGGER.info("Starting...");

		// Command line options
		final Options options = new Options();
		setupCliOptions(options);

		final CommandLineParser parser = new DefaultParser();
		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, argv);
		} catch (ParseException pe) {
			LOGGER.error(pe.getMessage());
			formatter.printHelp(ExpImpPipe.class.getCanonicalName(), options);
			System.exit(1);
		}

		final String sourceUrl = cmd.getOptionValue("source-jdbc-url");
		final String sourceUser = cmd.getOptionValue("source-user");
		final String sourcePassword = cmd.getOptionValue("source-password");
		PipePool source = null;
		try {
			source = PipePool.get("exp-pipe-pool", sourceUrl, sourceUser, sourcePassword);
		} catch (SQLException sqle) {
			LOGGER.error("Unable to connect to source database {}!", sourceUrl);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			System.exit(1);
		}
		LOGGER.info("Connected to source database '{}' as user '{}'.",
				sourceUrl, sourceUser);

		final String destUrl = cmd.getOptionValue("destination-jdbc-url");
		final String destUser = cmd.getOptionValue("destination-user");
		final String destPassword = cmd.getOptionValue("destination-password");
		PipePool dest = null;
		try {
			dest = PipePool.get("imp-pipe-pool", destUrl, destUser, destPassword);
		} catch (SQLException sqle) {
			LOGGER.error("Unable to connect to destination database {}!", destUrl);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			System.exit(1);
		}
		LOGGER.info("Connected to destination database '{}' as user '{}'.",
				destUrl, destUser);

		final String sourceSchema = cmd.getOptionValue("source-schema");
		final String sourceTable = cmd.getOptionValue("source-table");
		final String whereClause;
		if (StringUtils.isBlank(cmd.getOptionValue("where-clause"))) {
			whereClause = null;
		} else {
			whereClause = cmd.getOptionValue("where-clause");
		}
		final String destinationSchema;
		if (StringUtils.isBlank(cmd.getOptionValue("destination-schema"))) {
			destinationSchema = sourceSchema;
		} else {
			destinationSchema = cmd.getOptionValue("destination-schema");
		}
		final String destinationTable;
		if (StringUtils.isBlank(cmd.getOptionValue("destination-table"))) {
			destinationTable = sourceTable;
		} else {
			destinationTable = cmd.getOptionValue("destination-table");
		}

		int degree = 1;
		if (StringUtils.isBlank(cmd.getOptionValue("parallel-degree"))) {
			degree = Math.min(Runtime.getRuntime().availableProcessors(),
					Math.min(source.getDbCoreCount(), dest.getDbCoreCount()));
			LOGGER.info("Parallel degree will be set to {}.", degree);
		} else {
			try {
				degree = Integer.parseInt(cmd.getOptionValue("parallel-degree"));
			} catch (NumberFormatException nfe) {
				LOGGER.error("Unable to parse value of parallel-degree '{}'!", cmd.getOptionValue("parallel-degree"));
				LOGGER.error(nfe.getMessage());
				System.exit(1);
			}
		}
		try {
			source.setPoolSize(degree);
			dest.setPoolSize(degree);
		} catch (SQLException sqle) {
			LOGGER.error("Unable to change pool size to {}!", degree);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			System.exit(1);
		}

		int commitAfter = ROWS_TO_COMMIT;
		if (StringUtils.isBlank(cmd.getOptionValue("commit-after"))) {
			LOGGER.info("Commit will be executed after processing {} rows.", commitAfter);
		} else {
			try {
				commitAfter = Integer.parseInt(cmd.getOptionValue("commit-after"));
			} catch (NumberFormatException nfe) {
				LOGGER.error("Unable to parse value of commit-after '{}'!", cmd.getOptionValue("commit-after"));
				LOGGER.error(nfe.getMessage());
				System.exit(1);
			}
		}

		final boolean useDefaultFetchSize;
		if (cmd.hasOption("fetch-all-rows")) {
			useDefaultFetchSize = false;
		} else {
			useDefaultFetchSize = true;
		}

		final int rowIdStoreType;
		if (cmd.hasOption("use-chronicle-queue")) {
			rowIdStoreType = ROWID_STORE_CQ;
		} else {
			rowIdStoreType = ROWID_STORE_LIST;
		}
	
		final String rowIdColumnName;
		if (cmd.hasOption("add-rowid-to-dest")) {
			if (StringUtils.isBlank(cmd.getOptionValue("rowid-column-name"))) {
				rowIdColumnName = ROWID_KEY;
			} else {
				rowIdColumnName = cmd.getOptionValue("rowid-column-name");
			}
		} else {
			rowIdColumnName = null;
		}

		PipeTable table = null;
		try (OracleConnection connection = source.getConnection()) {
			//TODO
			table = new PipeTable(connection, sourceSchema, sourceTable,
					destinationSchema, destinationTable, whereClause,
					rowIdStoreType, rowIdColumnName);
		} catch (SQLException sqle) {
			LOGGER.error("Unable to read table {}.{} definition!",
					sourceSchema, sourceTable);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			System.exit(1);
		} catch (IOException ioe) {
			LOGGER.error("Unable to create Chronicle Queue!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
			System.exit(1);
		}

		if (table.rowCount() == 0) {
			LOGGER.info("Table {} does not contain any data!");
			table.close();
		} else {
			if (degree > table.rowCount()) {
				degree = table.rowCount();
				LOGGER.info("Parallel degree is set to {}.", degree);
			}
			final ExecutorService threadPool = Executors.newFixedThreadPool(degree);
			final CountDownLatch latch = new CountDownLatch(degree);
			int rowNumStart = 0;
			final int interval = Math.floorDiv(table.rowCount(), degree) + 1;
			for (int i = 0; i < degree; i++) {
				try {
					final int rownumEnd;
					if (i == degree - 1) {
						rownumEnd = table.rowCount();
					} else {
						rownumEnd = rowNumStart + interval;
					}
					final PipeWorker worker = new PipeWorker(i, latch,
							rowNumStart, rownumEnd, table, source.getConnection(),
							dest.getConnection(), commitAfter, useDefaultFetchSize);
					threadPool.submit(worker);
					rowNumStart = rownumEnd;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			threadPool.shutdown();
			table.close();
		}
	}

	private static void setupCliOptions(final Options options) {
		// Source connection
		final Option sourceJdbcUrl = new Option("src", "source-jdbc-url", true,
				"Oracle JDBC URL of source connection");
		sourceJdbcUrl.setRequired(true);
		options.addOption(sourceJdbcUrl);
		final Option sourceUser = new Option("u", "source-user", true,
				"Oracle user for source connection");
		sourceUser.setRequired(true);
		options.addOption(sourceUser);
		final Option sourcePassword = new Option("p", "source-password", true,
				"Password for source connection");
		sourcePassword.setRequired(true);
		options.addOption(sourcePassword);
		// Destination connection
		final Option destJdbcUrl = new Option("dest", "destination-jdbc-url", true,
				"Oracle JDBC URL of destination connection");
		destJdbcUrl.setRequired(true);
		options.addOption(destJdbcUrl);
		final Option destUser = new Option("U", "destination-user", true,
				"Oracle user for destination connection");
		destUser.setRequired(true);
		options.addOption(destUser);
		final Option destPassword = new Option("P", "destination-password", true,
				"Password for destination connection");
		destPassword.setRequired(true);
		options.addOption(destPassword);
		// Object description
		final Option sourceSchema = new Option("s", "source-schema", true,
				"Source schema name");
		sourceSchema.setRequired(true);
		options.addOption(sourceSchema);
		final Option sourceTable = new Option("t", "source-table", true,
				"Source table name");
		sourceTable.setRequired(true);
		options.addOption(sourceTable);
		final Option destSchema = new Option("S", "destination-schema", true,
				"Destination schema name, if not specified value of --source-schema used");
		destSchema.setRequired(false);
		options.addOption(destSchema);
		final Option destTable = new Option("T", "destination-table", true,
				"Destination table name, if not specified value of --source-table used");
		destTable.setRequired(false);
		options.addOption(destTable);
		final Option whereClause = new Option("w", "where-clause", true,
				"Optional where clause for source table");
		whereClause.setRequired(false);
		options.addOption(whereClause);
		// Optional - parallelism...
		final Option parallelDegree = new Option("d", "parallel-degree", true,
				"Optional number of parallel workers to run");
		parallelDegree.setRequired(false);
		options.addOption(parallelDegree);
		// Optional - rows to commit...
		final Option rowsToCommit = new Option("c", "commit-after", true,
				"Optional number of rows to commit. Default value - " + ROWS_TO_COMMIT);
		rowsToCommit.setRequired(false);
		options.addOption(rowsToCommit);
		// Optional - fetch all
		final Option prefetchAllRows = new Option("f", "fetch-all-rows", false,
				"When specified try to fetch all rows from source");
		prefetchAllRows.setRequired(false);
		options.addOption(prefetchAllRows);
		// Optional - use ChronicleQueue to store ROWID
		final Option useChronicle = new Option("q", "use-chronicle-queue", false,
				"When specified Chronicle Queue will be used as temporary store for ROWIDs");
		useChronicle.setRequired(false);
		options.addOption(useChronicle);
		// Optional - add ROWID data
		final Option addRowId = new Option("r", "add-rowid-to-dest", false,
				"When specified ROWID pseudocolumn is added to destination as VARCHAR column with name ORA_ROW_ID");
		addRowId.setRequired(false);
		options.addOption(addRowId);
		final Option rowIdColumnName = new Option("n", "rowid-column-name", true,
				"Specifies the name for the column in destination table storing the source ROWIDs. Default - " + ROWID_KEY);
		rowIdColumnName.setRequired(false);
		options.addOption(rowIdColumnName);

	}
}
