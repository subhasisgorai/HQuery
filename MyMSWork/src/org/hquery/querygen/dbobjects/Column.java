package org.hquery.querygen.dbobjects;

import static org.hquery.common.util.HQueryConstants.DOT;
import static org.hquery.common.util.HQueryConstants.EMPTY_STRING;
import static org.hquery.common.util.HQueryConstants.ENDING_BRACE;
import static org.hquery.common.util.HQueryConstants.STARTING_BRACE;
import static org.hquery.common.util.HQueryConstants.AS_STRING;

import org.apache.commons.lang.StringUtils;
import org.hquery.querygen.visitor.QueryElement;
import org.hquery.querygen.visitor.QueryElementVisitor;

public class Column implements QueryElement {

	private String columnName;
	private DataType dataType;
	private Table owningTable;
	private String functionName;
	private String alias;

	public String getAlias() {
		return alias;
	}

	public Column setAlias(String alias) {
		this.alias = alias;
		return this;
	}

	public boolean hasFunction() {
		return (!StringUtils.isBlank(functionName));
	}

	public String getFunctionName() {
		return functionName;
	}

	public Column setFunctionName(String functionName) {
		this.functionName = functionName;
		return this;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType columnType) {
		this.dataType = columnType;
	}

	public enum DataType {
		TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, STRING
	}

	public enum ColumnType {
		PROJECTED_TYPE, GROUP_BY_TYPE, ORDER_BY_TYPE;
	}

	public Column() {

	}

	public Column(String columnName, DataType columnType) {
		this.columnName = columnName;
		this.dataType = columnType;
	}

	public Column(String columnName, DataType columnType, String alias) {
		this.columnName = columnName;
		this.dataType = columnType;
		this.alias = alias;
	}

	public Table getOwningTable() {
		return owningTable;
	}

	public Column setOwningTable(Table owningTable) {
		this.owningTable = owningTable;
		return this;
	}

	@Override
	public void accept(QueryElementVisitor visitor) {
		visitor.visit(this);
	}

	public String getColumnString() {
		StringBuffer columnString = new StringBuffer()
				.append((hasFunction()) ? functionName : EMPTY_STRING)
				.append((hasFunction()) ? STARTING_BRACE : EMPTY_STRING)
				.append(this.getOwningTable())
				.append(DOT)
				.append(this.getColumnName())
				.append((hasFunction()) ? ENDING_BRACE : EMPTY_STRING)
				.append(!(StringUtils.isBlank(this.getAlias())) ? AS_STRING
						: EMPTY_STRING)
				.append(!(StringUtils.isBlank(this.getAlias())) ? this
						.getAlias() : EMPTY_STRING);
		return columnString.toString();
	}

	public String toString() {
		return this.getColumnName();
	}
}
