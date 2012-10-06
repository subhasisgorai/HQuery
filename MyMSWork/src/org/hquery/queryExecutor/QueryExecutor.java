package org.hquery.queryExecutor;

import org.hquery.common.util.Context;
import org.hquery.status.impl.JobStatusCheckerImpl.StatusCheckerThread;

public interface QueryExecutor {
	public abstract void setQuery(String queryString);

	public abstract String executeQuery(Context context);
	
	public abstract void setStatusCheckerThread(StatusCheckerThread statusCheckerThread);
}
