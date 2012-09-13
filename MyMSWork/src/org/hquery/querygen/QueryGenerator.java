package org.hquery.querygen;

import org.hquery.querygen.query.Query;

public interface QueryGenerator {

	public void generateQuery();
	public String getQueryString();
	public Query getQueryObject();
	public void setQueryObject(Query queryObejct);

}
