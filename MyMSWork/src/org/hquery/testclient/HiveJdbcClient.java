package org.hquery.testclient;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.hquery.querygen.QueryGenerator;
import org.hquery.querygen.dbobjects.Column;
import org.hquery.querygen.dbobjects.Column.DataType;
import org.hquery.querygen.dbobjects.LogicalOperator;
import org.hquery.querygen.dbobjects.Table;
import org.hquery.querygen.dialect.HiveDialect;
import org.hquery.querygen.filter.Filter;
import org.hquery.querygen.filter.decorator.FilterDecorator;
import org.hquery.querygen.impl.HiveQueryGenerator;
import org.hquery.querygen.query.Query;
import org.hquery.querygen.query.QueryType;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private boolean writeToFile = true;
	private String fileLocation = "/user/subhasig/my_hadoop_store/result";
	private String referenceSql = ((writeToFile) ? "insert overwrite directory '"
			+ fileLocation + "'"
			: "")
			+ "select year, max(temperature) from records where temperature != 9999 "
			+ "and (quality=0 or quality=1 or quality=4 or quality=5 or quality=9) group by year";
	private static String sql = null;
	private int prevState = -1;

	public static void main(String[] args) throws SQLException,
			InterruptedException {

		Query query = new Query();

		Table records = new Table("records");
		Column year = new Column("year", DataType.STRING)
				.setOwningTable(records);
		Column temperature = new Column("temperature", DataType.INT)
				.setOwningTable(records).setFunctionName("max");
		Column quality = new Column("quality", DataType.INT)
				.setOwningTable(records);
		records.addProjectedColumn(year);
		records.addProjectedColumn(temperature);
		records.addGroupByColumn(year);

		Filter filter = new Filter();
		filter.setColumn(quality);
		filter.setOperator(LogicalOperator.EQ);
		filter.setValue("9");

		FilterDecorator firstDecorator = new FilterDecorator(quality,
				LogicalOperator.EQ, "5");
		firstDecorator.setFilter(filter);
		firstDecorator.setOuterOperator(LogicalOperator.OR);

		FilterDecorator secondDecorator = new FilterDecorator(quality,
				LogicalOperator.EQ, "4");
		secondDecorator.setFilter(firstDecorator);
		secondDecorator.setOuterOperator(LogicalOperator.OR);

		FilterDecorator thirdDecorator = new FilterDecorator(quality,
				LogicalOperator.EQ, "1");
		thirdDecorator.setFilter(secondDecorator);
		thirdDecorator.setOuterOperator(LogicalOperator.OR);

		FilterDecorator fourhDecorator = new FilterDecorator(quality,
				LogicalOperator.EQ, "1");
		fourhDecorator.setFilter(thirdDecorator);
		fourhDecorator.setOuterOperator(LogicalOperator.OR);

		FilterDecorator fifthDecorator = new FilterDecorator(quality,
				LogicalOperator.EQ, "0");
		fifthDecorator.setFilter(fourhDecorator);
		fifthDecorator.setOuterOperator(LogicalOperator.OR);

		FilterDecorator sixthDecorator = new FilterDecorator(temperature,
				LogicalOperator.NOT_EQ, "9999");
		sixthDecorator.setFilter(fifthDecorator.setGrouped(true));
		sixthDecorator.setOuterOperator(LogicalOperator.AND);

		records.setFilter(sixthDecorator);
		query.setTable(records);
		query.setQueryType(QueryType.SELECT_QUERY);

		QueryGenerator queryGenerator = new HiveQueryGenerator(
				new HiveDialect());

		queryGenerator.setQueryObject(query);
		queryGenerator.generateQuery();

		sql = queryGenerator.getQueryString();

		HiveConf conf = new HiveConf(HiveJdbcClient.class);
		SessionState.start(conf);
		SessionState sessionState = SessionState.get();
		String sessionId = sessionState.getSessionId();
		System.out.println("Session Id: " + sessionId);
		HiveMetaStoreClient metaStoreClient = null;
		if (conf != null)
			System.out.println("Hive configuration initialized successfully");
		try {
			metaStoreClient = new HiveMetaStoreClient(conf);
			System.out.println("Databases: "
					+ metaStoreClient.getAllDatabases());
			System.out.println("Tables: "
					+ metaStoreClient.getAllTables("default"));
		} catch (MetaException e) {
			e.printStackTrace();
		} finally {
			metaStoreClient.close();
		}

		HiveJdbcClient hiveJdbcClient = new HiveJdbcClient();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Runnable job = hiveJdbcClient.new HiveExecutor() {
			@Override
			public void handleResultSet(ResultSet res) {
				if (res != null) {
					try {
						System.out.println("\nResult:");
						while (res.next()) {
							System.out.println(res.getString(1) + "\t"
									+ res.getString(2));
						}
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				}
			}

		};
		executor.submit(job);
		System.out.println("\nNow submitted the job");
		executor.shutdown();

		ScheduledExecutorService executorService = Executors
				.newSingleThreadScheduledExecutor(new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = new Thread(r);
						thread.setDaemon(true);
						return thread;
					}
				});
		executorService.scheduleWithFixedDelay(
				hiveJdbcClient.new StatusChecker(sessionId), 0, 2,
				TimeUnit.SECONDS);
		System.out.println("\n[main] thread terminating NOW");
		System.out.println("\nJob Status:");

	}

	public class StatusChecker implements Runnable {
		String sessionId;

		public StatusChecker(String sessionId) {
			this.sessionId = sessionId;
		}

		JobID jobIdUnderObservation = null;

		@Override
		public void run() {
			JobClient jobClient;
			JobStatus[] statuses = null;
			Configuration conf = new Configuration();
			InputStream is = null;
			try {
				jobClient = new JobClient(new InetSocketAddress(
						InetAddress.getByName("127.0.0.1"), 9001),
						new Configuration());
				statuses = jobClient.getJobsFromQueue("default");
				if (jobIdUnderObservation == null) {
					System.out.print("\nUnknown");
					for (JobStatus status : statuses) {
						JobID jobId = status.getJobID();
						RunningJob runningJob = jobClient.getJob(jobId);
						if (runningJob != null) {
							String jobFile = runningJob.getJobFile();
							FileSystem fs = FileSystem.get(URI.create(jobFile),
									conf);
							if (fs.exists(new Path(jobFile))) {
								is = fs.open(new Path(jobFile));
								conf.addResource(is);
								String sessionId = conf.get("hive.session.id");
								if (this.sessionId.equals(sessionId)) {
									jobIdUnderObservation = jobId;
								}
							}

						}
					}
				} else {
					for (JobStatus status : statuses) {
						JobID jobId = status.getJobID();
						if (jobId.equals(jobIdUnderObservation)) {
							int jobStatus = status.getRunState();
							if (jobStatus != prevState) {
								prevState = jobStatus;
								switch (jobStatus) {
								case JobStatus.PREP:
									System.out.print("\nPreparing ");
									break;
								case JobStatus.SUCCEEDED:
									System.out.print("\nFinished");
									break;
								case JobStatus.FAILED:
									System.out.print("\nFailed");
									break;
								case JobStatus.KILLED:
									System.out.print("\nKilled");
									break;
								case JobStatus.RUNNING:
								default:
									System.out.print("\nRunning\n");
								}
							} else {
								if (status.getRunState() == JobStatus.RUNNING) {
									System.out
											.printf("\tMap progress: %.1f%%, Reduce Progress: %.1f%%%n",
													status.mapProgress() * 100,
													status.reduceProgress() * 100);
								} else
									System.out.print("..");
							}
						}
					}

				}
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				IOUtils.closeStream(is);
			}
		}
	}

	public abstract class HiveExecutor implements Runnable {

		@Override
		public void run() {
			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				System.exit(1);
			}
			Connection con = null;
			Statement stmt = null;
			ResultSet res = null;
			try {
				con = DriverManager.getConnection(
						"jdbc:hive://localhost:10000/default", "", "");
				stmt = con.createStatement();
				System.out.println("Reference Query\n" + referenceSql);
				System.out.println("Executing folowing query: \n" + sql);
				res = stmt.executeQuery(sql);
				System.out
						.println("\nExecution over, processing the result ... ");
				handleResultSet(res);
				System.out
						.println("\nProcessing over, worker thread terminating now");
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					res.close();
					stmt.close();
					con.close();
				} catch (SQLException e) {
					System.err
							.println("Error encountered when trying to close resultset/statement/connection: ["
									+ e.getMessage() + "]");
				}
			}
		}

		public abstract void handleResultSet(ResultSet res);

	}
}
