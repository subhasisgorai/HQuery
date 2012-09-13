package org.hquery.querygen.dbobjects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.hquery.querygen.filter.Filter;
import org.hquery.querygen.visitor.QueryElement;
import org.hquery.querygen.visitor.QueryElementVisitor;

public class Table implements QueryElement {

	public Table(String tableName) {
		this.tableName = tableName;
	}

	protected List<Column> projectedColumns;
	protected List<Column> groupByColumns;
	protected Map<Column, Map<Table, Column>> joinStructure;

	private String tableName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<Column> getProjectedColumns() {
		return projectedColumns;
	}

	public void setProjectedColumns(List<Column> columns) {
		this.projectedColumns = columns;
	}

	public Map<Column, Map<Table, Column>> getJoinStructure() {
		return joinStructure;
	}

	public void setJoinStructure(Map<Column, Map<Table, Column>> joinStructure) {
		this.joinStructure = joinStructure;
	}

	public void addProjectedColumn(Column column) {
		if (projectedColumns == null)
			projectedColumns = new ArrayList<Column>();
		projectedColumns.add(column);
	}

	public Table joinTable(Table joinTable, Column srcJoiningColumn,
			Column destJoiningColumn) {
		if (joinStructure == null)
			joinStructure = new HashMap<Column, Map<Table, Column>>();
		Map<Table, Column> joinTables = joinStructure.get(srcJoiningColumn);
		if (joinTables == null) {
			joinTables = new HashMap<Table, Column>();
		}
		joinTables.put(joinTable, destJoiningColumn);
		joinStructure.put(srcJoiningColumn, joinTables);
		return this;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37).append(tableName).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Table == false) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		final Table otherObject = (Table) obj;
		return new EqualsBuilder().append(this.tableName,
				otherObject.getTableName()).isEquals();
	}

	public String toString() {
		return this.getTableName();
	}

	@Override
	public void accept(QueryElementVisitor visitor) {
		for (Column column : projectedColumns) {
			column.accept(visitor);
		}

		visitor.setProcessingProjectedColumnsOver(true);

		for (Column column : groupByColumns) {
			column.accept(visitor);
		}

		visitor.visit(this);
	}

	private Filter filter;

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	private boolean isDistinct;

	public boolean isDistinct() {
		return isDistinct;
	}

	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}

	public List<Column> getGroupByColumns() {
		return groupByColumns;
	}

	public void setGroupByColumns(List<Column> groupByColumns) {
		this.groupByColumns = groupByColumns;
	}

	public void addGroupByColumn(Column column) {
		if (groupByColumns == null)
			groupByColumns = new ArrayList<Column>();
		groupByColumns.add(column);
	}

}
