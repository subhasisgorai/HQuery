package org.hquery.assembler;

import static org.hquery.common.util.HQueryConstants.QUERY;
import static org.hquery.common.util.HQueryConstants.USER_PREF;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobStatus;
import org.hquery.common.util.Context;
import org.hquery.common.util.HQueryUtil;
import org.hquery.common.util.UserPreferences;
import org.hquery.metastore.MetaInformationService;
import org.hquery.queryExecutor.QueryExecutor;
import org.hquery.querygen.QueryGenerator;
import org.hquery.querygen.query.Query;
import org.hquery.status.StatusChecker;

public class HQueryAssembler {
	private QueryGenerator queryGenerator;
	private QueryExecutor queryExecutor;
	private StatusChecker statusChecker;
	private MetaInformationService metaInformationService;

	public void setQueryGenerator(QueryGenerator queryGenerator) {
		this.queryGenerator = queryGenerator;
	}

	public void setQueryExecutor(QueryExecutor queryExecutor) {
		this.queryExecutor = queryExecutor;
	}

	public void setStatusChecker(StatusChecker statusChecker) {
		this.statusChecker = statusChecker;
	}

	public void setMetaInformationService(
			MetaInformationService metaInformationService) {
		this.metaInformationService = metaInformationService;
	}

	public MetaInformationService getMetaInformationService() {
		return this.metaInformationService;
	}

	public String getQueryString(Query queryObject) {
		assert (queryObject != null) : "Query Object shouldn't be null for query formation";
		queryGenerator.setQueryObject(queryObject);
		queryGenerator.generateQuery();
		return queryGenerator.getQueryString();
	}

	public String executeQuery(Query queryObject, UserPreferences preferences) {
		return executeQuery(getQueryString(queryObject), preferences);
	}

	public String executeQuery(String queryString, UserPreferences preferences) {
		assert (StringUtils.isNotBlank(queryString)) : "Query String shouldn't be null for execute query";
		Context context = new Context();
		context.putInContext(QUERY, queryString);
		context.putInContext(USER_PREF, preferences);
		String sessionId = queryExecutor.executeQuery(context);
		statusChecker.intiateStatusCheck(sessionId);
		return sessionId;
	}

	public String checkStatus(String sessionId) {
		assert (StringUtils.isNotBlank(sessionId)) : "Session id shouldn't be null for status checking";
		return statusChecker.checkStatus(sessionId);
	}

	public JobStatus getStatus(String sessionId) {
		assert (StringUtils.isNotBlank(sessionId)) : "Session id shouldn't be null for getting status";
		return statusChecker.getStatus(sessionId);
	}

	public void printRTStatus(String sessionId) {
		ExecutorService executorService = Executors
				.newSingleThreadExecutor(new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = new Thread(r);
						thread.setDaemon(true);
						return thread;
					}
				});
		executorService.submit(this.new StatusCheckerDaemon(sessionId));
	}

	private volatile Map<String, Integer> statusMap;

	public class StatusCheckerDaemon implements Runnable {
		private String sessionId;

		public StatusCheckerDaemon(String sessionId) {
			this.sessionId = sessionId;
		}

		@Override
		public void run() {
			System.out.println("\n");
			System.out.println("Job Progress Status: ");
			System.out.print("Unknown");

			JobStatus currentStatus = getStatus(sessionId);
			while (currentStatus == null) {
				System.out.print("..");
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				currentStatus = getStatus(sessionId);
			}

			while (!currentStatus.isJobComplete()) {
				if (statusMap == null)
					statusMap = new HashMap<String, Integer>();
				if (statusMap.get(sessionId) == null
						|| statusMap.get(sessionId).intValue() != currentStatus
								.getRunState()) {
					synchronized (statusMap) {
						statusMap.put(sessionId, currentStatus.getRunState());
					}
					if (currentStatus.getRunState() == JobStatus.PREP)
						System.out.print("\nPreparing ");
					else if (currentStatus.getRunState() == JobStatus.RUNNING) {
						System.out.println("\nRunning");
					}
				} else {
					if (currentStatus.getRunState() == JobStatus.RUNNING) {
						System.out
								.printf("\tMap progress: %.1f%%, Reduce Progress: %.1f%%%n",
										currentStatus.mapProgress() * 100,
										currentStatus.reduceProgress() * 100);
					} else {
						System.out.print("..");
					}
				}
				try {
					Thread.sleep(Long.parseLong(HQueryUtil.getResourceString(
							"hquery-conf", "status.checker.monitor.interval")));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				currentStatus = getStatus(sessionId);
			}
			System.out.println(checkStatus(sessionId));

		}

	}

}
