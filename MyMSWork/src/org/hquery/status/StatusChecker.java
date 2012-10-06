package org.hquery.status;

import java.util.List;

import org.apache.hadoop.mapred.JobStatus;
import org.hquery.status.impl.JobStatusCheckerImpl.StatusCheckerThread;
import org.hquery.status.impl.JobStatusCheckerImpl.StatusEnum;

public interface StatusChecker {
	public abstract StatusCheckerThread intiateStatusCheck(String sessionId);

	public abstract StatusEnum checkStatus(String sessionId);

	public abstract List<JobStatus> getStatus(String sessionId);
}
