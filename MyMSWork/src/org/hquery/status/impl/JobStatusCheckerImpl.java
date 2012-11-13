package org.hquery.status.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;
import org.hquery.common.util.HQueryUtil;
import org.hquery.status.StatusChecker;
import org.mortbay.log.Log;

public class JobStatusCheckerImpl implements StatusChecker {

	private static Logger logger = Logger.getLogger(JobStatusCheckerImpl.class);

	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	private Map<String, List<JobStatus>> statusMap;
	private volatile Map<String, Boolean> completedMap;
	private static final int NTHREADS = Integer
			.parseInt(HQueryUtil.getResourceString("hquery-conf",
					"status.checker.threadpool.size"));
	ExecutorService executor = Executors.newFixedThreadPool(NTHREADS,
			new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread thread = new Thread(r);
					thread.setDaemon(true);
					return thread;
				}
			});

	@Override
	public StatusCheckerThread intiateStatusCheck(String sessionId) {
		if (statusMap == null) {
			statusMap = new HashMap<String, List<JobStatus>>();
		}
		if (completedMap == null) {
			completedMap = new HashMap<String, Boolean>();
		}
		StatusCheckerThread thread = new StatusCheckerThread(sessionId);
		executor.submit(thread);
		return thread;

	}

	@Override
	public StatusEnum checkStatus(String sessionId) {
		if (getCompletedStatus(sessionId))
			return StatusEnum.COMPLETED;
		else if (!getCompletedStatus(sessionId)
				&& !(getStatus(sessionId).isEmpty()))
			return StatusEnum.RUNNING;
		else
			return StatusEnum.UNKNOWN;
	}

	@Override
	public List<JobStatus> getStatus(String sessionId) {
		return new ArrayList<JobStatus>(getStatusList(sessionId));
	}

	private boolean getCompletedStatus(String sessionId) {
		assert (sessionId != null) : "Session Id for status checking shouldn't be null";
		if (completedMap != null && completedMap.containsKey(sessionId))
			return completedMap.get(sessionId);
		else
			return false;
	}

	private List<JobStatus> getStatusList(String sessionId) {
		read.lock();
		try {
			return (statusMap != null && statusMap.get(sessionId) != null) ? statusMap
					.get(sessionId) : Collections.<JobStatus> emptyList();

		} finally {
			read.unlock();
		}
	}

	private void updateStatus(String sessionId, JobStatus newStatus) {
		List<JobStatus> statusList = getStatusList(sessionId);
		if (CollectionUtils.isEmpty(statusList)) {
			write.lock();
			try {
				statusList = new ArrayList<JobStatus>();
				statusList.add(newStatus);
				statusMap.put(sessionId, statusList);
			} finally {
				write.unlock();
			}
		} else {
			write.lock();
			try {
				boolean inserted = false;
				for (JobStatus oldSatus : statusList) {
					if (oldSatus.getJobID().equals(newStatus.getJobID())) {
						statusList.remove(oldSatus);
						statusList.add(newStatus);
						inserted = true;
						break;
					}
				}
				if (!inserted)
					statusList.add(newStatus);
			} finally {
				write.unlock();
			}
		}
	}

	public class StatusCheckerThread implements Runnable {
		private String sessionId;
		private Set<JobID> jobIdsUnderObservation = new HashSet<JobID>();
		private boolean stopRequested = false;

		public synchronized void requestStop() {
			this.stopRequested = true;
			JobStatusCheckerImpl.this.completedMap.put(sessionId, true);
		}

		public synchronized boolean stopRequested() {
			return this.stopRequested;
		}

		public StatusCheckerThread(String sessionId) {
			this.sessionId = sessionId;
		}

		@Override
		public void run() {
			final JobClient jobClient;
			JobStatus[] statuses = null;
			String jobTrackerhost = HQueryUtil.getResourceString("hquery-conf",
					"job.tracker.host");
			String jobTrackerPort = HQueryUtil.getResourceString("hquery-conf",
					"job.tracker.port");
			String jobQueue = HQueryUtil.getResourceString("hquery-conf",
					"job.queue");

			assert (StringUtils.isNotBlank(jobTrackerhost)) : "Job Tracker host name can't be null";
			assert (StringUtils.isNotBlank(jobTrackerPort)) : "Job Tracker port name can't be null";
			assert (StringUtils.isNotBlank(jobQueue)) : "Job Queue can't be null";

			try {
				jobClient = new JobClient(new InetSocketAddress(
						InetAddress.getByName(jobTrackerhost),
						Integer.parseInt(jobTrackerPort)), new Configuration());
			} catch (Exception ex) {
				logger.fatal("Could n't intialie the Job client", ex);
				throw new RuntimeException("Could n't intialie the Job client");
			}

			while (getStatusList(this.sessionId) == null || !stopRequested()) { // if there is no status for the job or if the job is not complete

				try {
					statuses = jobClient.getJobsFromQueue(jobQueue); // get statuses for all the jobs in queue
				} catch (IOException e) {
					e.printStackTrace();
				}

				if (statuses != null && statuses.length > 0) {
					for (JobStatus status : statuses) { //TODO to get for a particular user to reduce scope for searching
						JobID jobId = status.getJobID(); //TODO check the hash-code/equals implementation of JobID
						if (!jobIdsUnderObservation.contains(jobId)) { //explore job if it's new
							class JobFinder implements Runnable {
								JobID jobId;

								public JobFinder(JobID jobId) {
									this.jobId = jobId;
								}

								Configuration conf = new Configuration();

								@Override
								public void run() {
									try {
										RunningJob runningJob = jobClient
												.getJob(this.jobId);
										if (runningJob != null) {
											String jobFile = runningJob
													.getJobFile();
											if (logger.isDebugEnabled())
												Log.debug("Job file location ["
														+ jobFile + "]");
											FileSystem fs = FileSystem
													.get(conf);
											if (fs.exists(new Path(jobFile))) {
												InputStream is = fs
														.open(new Path(jobFile));
												conf.addResource(is);
												String sessionId = conf
														.get("hive.session.id");
												if (sessionId
														.equals(StatusCheckerThread.this.sessionId)) {
													jobIdsUnderObservation
															.add(jobId);
												}
											}

										}
									} catch (Throwable t) {
										logger.debug(
												"Error encountered while reading job file",
												t);
									}

								}
							}
							;
							Thread t = new Thread(new JobFinder(jobId));
							t.start();
						}
						int prevStatus = 0;
						if (jobIdsUnderObservation.contains(jobId)) {
							for (JobStatus jobStatus : getStatusList(this.sessionId)) {
								if (jobStatus.getJobID().equals(jobId)) {
									prevStatus = jobStatus.getRunState();
									break;
								}
							}

							int currentJobStatus = status.getRunState();

							if (currentJobStatus != prevStatus
									|| currentJobStatus == JobStatus.RUNNING) {
								updateStatus(sessionId, status);
							}
						}
					}
				}
			}
		}
	}

	public enum StatusEnum {

		RUNNING {
			public String toString() {
				return "Running";
			}
		},

		COMPLETED {
			public String toString() {
				return "Completed";
			}
		},
		UNKNOWN;

		public String toString() {
			return "Unknown";
		}
	}

}
