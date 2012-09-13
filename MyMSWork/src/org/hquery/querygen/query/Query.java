package org.hquery.querygen.query;

import org.hquery.querygen.dbobjects.Table;

public class Query {
	private QueryType queryType;
	private Table table;

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public QueryType getQueryType() {
		return this.queryType;
	}

	public void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}
}
