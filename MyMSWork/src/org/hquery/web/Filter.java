package org.hquery.web;

import org.hquery.querygen.dbobjects.LogicalOperator;

public class Filter {
	private LogicalOperator outerOperator;
	private Column column;
	private LogicalOperator innerOperator;
	private String value;

	public LogicalOperator getOuterOperator() {
		return outerOperator;
	}

	public void setOuterOperator(LogicalOperator outerOperator) {
		this.outerOperator = outerOperator;
	}

	public Column getColumn() {
		return column;
	}

	public void setColumn(Column column) {
		this.column = column;
	}

	public LogicalOperator getInnerOperator() {
		return innerOperator;
	}

	public void setInnerOperator(LogicalOperator innerOperator) {
		this.innerOperator = innerOperator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
