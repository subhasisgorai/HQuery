package org.hquery.status;

import org.apache.hadoop.mapred.JobStatus;

public interface StatusChecker {
	public abstract void intiateStatusCheck(String sessionId);

	public abstract String checkStatus(String sessionId);

	public abstract JobStatus getStatus(String sessionId);
}
