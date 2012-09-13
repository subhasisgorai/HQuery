package org.hquery.status.impl;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.hquery.common.util.HQueryUtil;
import org.hquery.status.StatusChecker;

public class JobStatusCheckerImpl implements StatusChecker {

	private volatile Map<String, JobStatus> statusMap;
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
	public void intiateStatusCheck(String sessionId) {
		if (statusMap == null) {
			statusMap = new HashMap<String, JobStatus>();
		}

		executor.submit(new StatusCheckerThread(sessionId));

	}

	@Override
	public String checkStatus(String sessionId) {
		return JobStatus.getJobRunState(statusMap.get(sessionId).getRunState());
	}

	@Override
	public JobStatus getStatus(String sessionId) {
		return statusMap.get(sessionId);
	}

	public class StatusCheckerThread implements Runnable {
		private String sessionId;
		private JobID jobIdUnderObservation = null;

		public StatusCheckerThread(String sessionId) {
			this.sessionId = sessionId;
		}

		@Override
		public void run() {
			JobClient jobClient;
			JobStatus[] statuses = null;
			Configuration conf = new Configuration();
			InputStream is = null;
			try {
				String jobTrackerhost = HQueryUtil.getResourceString(
						"hquery-conf", "job.tracker.host");
				String jobTrackerPort = HQueryUtil.getResourceString(
						"hquery-conf", "job.tracker.port");
				String jobQueue = HQueryUtil.getResourceString("hquery-conf",
						"job.queue");

				assert (StringUtils.isNotBlank(jobTrackerhost)) : "Job Tracker host name can't be null";
				assert (StringUtils.isNotBlank(jobTrackerPort)) : "Job Tracker port name can't be null";
				assert (StringUtils.isNotBlank(jobQueue)) : "Job Queue can't be null";

				jobClient = new JobClient(new InetSocketAddress(
						InetAddress.getByName(jobTrackerhost),
						Integer.parseInt(jobTrackerPort)), new Configuration());

				int prevStatus = 0; // initialize previous status

				while (statusMap.get(sessionId) == null
						|| !statusMap.get(sessionId).isJobComplete()) { // if there is no status for the job or if the job is not complete

					statuses = jobClient.getJobsFromQueue(jobQueue); // get statuses for all the jobs in queue

					if (jobIdUnderObservation == null) {
						for (JobStatus status : statuses) {
							JobID jobId = status.getJobID();
							RunningJob runningJob = jobClient.getJob(jobId);
							if (runningJob != null) {
								String jobFile = runningJob.getJobFile();
								FileSystem fs = FileSystem.get(
										URI.create(jobFile), conf);
								if (fs.exists(new Path(jobFile))) {
									is = fs.open(new Path(jobFile));
									conf.addResource(is);
									String sessionId = conf
											.get("hive.session.id");
									if (sessionId.equals(this.sessionId)) {
										jobIdUnderObservation = jobId;
									}
								}

							}
						}
					} else {
						for (JobStatus status : statuses) {
							JobID jobId = status.getJobID();
							if (jobId.equals(jobIdUnderObservation)) {
								if (statusMap.get(sessionId) != null) {
									prevStatus = statusMap.get(sessionId)
											.getRunState();
								}

								int currentJobStatus = status.getRunState();

								if (currentJobStatus != prevStatus
										|| currentJobStatus == JobStatus.RUNNING) {
									synchronized (statusMap) {
										statusMap.put(sessionId, status);
									}
								}
							}
						}

					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				if (is != null)
					IOUtils.closeStream(is);
			}
		}
	}

}
