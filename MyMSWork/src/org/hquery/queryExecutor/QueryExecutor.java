package org.hquery.queryExecutor;

import org.hquery.common.util.Context;

public interface QueryExecutor {
	public abstract void setQuery(String queryString);

	public abstract String executeQuery(Context context);
}
