package org.hquery.web;

public class ColumnFunction {
	private Column column;
	private String functionName;

	public Column getColumn() {
		return column;
	}

	public void setColumn(Column column) {
		this.column = column;
	}

	public String getFunctionName() {
		return functionName;
	}

	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	public ColumnFunction(Column column, String functionName) {
		this.column = column;
		this.functionName = functionName;
	}

}
