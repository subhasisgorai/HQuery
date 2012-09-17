package org.hquery.querygen.dbobjects;

import java.util.List;

import org.hquery.querygen.query.Query;

public class VirtualTable extends Table {

	public VirtualTable(String tableName) {
		super(tableName);
	}

	private Query query;
	private String QueryString;

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public String getQueryString() {
		return QueryString;
	}

	public void setQueryString(String queryString) {
		QueryString = queryString;
	}

	public List<Column> getColumns() {
		assert (query != null) : "Virtual table query can't be null";
		return query.getTable().getProjectedColumns();
	}

}
