package org.hquery.querygen.filter;

import static org.hquery.common.util.HQueryConstants.DOT;
import static org.hquery.common.util.HQueryConstants.ENDING_BRACE;
import static org.hquery.common.util.HQueryConstants.SINGLE_QUOTE;
import static org.hquery.common.util.HQueryConstants.SPACE_STRING;
import static org.hquery.common.util.HQueryConstants.STARTING_BRACE;

import org.apache.commons.lang.StringUtils;
import org.hquery.querygen.dbobjects.Column;
import org.hquery.querygen.dbobjects.LogicalOperator;
import org.hquery.querygen.exception.QueryFormationException;

public class Filter {
	protected Column column;
	protected LogicalOperator operator;
	protected String value;

	public Column getColumn() {
		return column;
	}

	public void setColumn(Column column) {
		this.column = column;
	}

	public LogicalOperator getOperator() {
		return operator;
	}

	public void setOperator(LogicalOperator operator) {
		this.operator = operator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getFilterString() {
		if (column != null && operator != null && !StringUtils.isBlank(value)) {
			return new StringBuffer().append(STARTING_BRACE)
					.append(column.getOwningTable().getTableName()).append(DOT)
					.append(column.getColumnName()).append(SPACE_STRING)
					.append(operator).append(SPACE_STRING).append(SINGLE_QUOTE)
					.append(value).append(SINGLE_QUOTE).append(ENDING_BRACE)
					.toString();
		}
		throw new QueryFormationException(
				"Error encountered while processing filters");
	}

	public Filter() {
	}

	public Filter(Column column, LogicalOperator operator, String value) {
		this.column = column;
		this.operator = operator;
		this.value = value;
	}

}
