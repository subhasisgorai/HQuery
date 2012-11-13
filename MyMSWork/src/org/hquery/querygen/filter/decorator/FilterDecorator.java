package org.hquery.querygen.filter.decorator;

import static org.hquery.common.util.HQueryConstants.ENDING_BRACE;
import static org.hquery.common.util.HQueryConstants.SPACE_STRING;
import static org.hquery.common.util.HQueryConstants.STARTING_BRACE;

import org.hquery.querygen.dbobjects.Column;
import org.hquery.querygen.dbobjects.LogicalOperator;
import org.hquery.querygen.exception.QueryFormationException;
import org.hquery.querygen.filter.Filter;

public class FilterDecorator extends Filter {
	private Filter filter;
	private FilterDecorator innerDecorator;
	private LogicalOperator outerOperator;
	private boolean isGrouped;

	public boolean isGrouped() {
		return isGrouped;
	}

	public FilterDecorator setGrouped(boolean isGrouped) {
		this.isGrouped = isGrouped;
		return this;
	}

	public void setOuterOperator(LogicalOperator outerOperator) {
		this.outerOperator = outerOperator;
	}

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		if (innerDecorator == null)
			this.filter = filter;
		else
			throw new RuntimeException("Inner decorator is already set");
	}

	public FilterDecorator getInnerDecorator() {
		return innerDecorator;
	}

	public void setInnerDecorator(FilterDecorator innerDecorator) {
		if (filter == null)
			this.innerDecorator = innerDecorator;
		else
			throw new RuntimeException("Inner filter is already set");
	}

	@Override
	public String getFilterString() {
		if (outerOperator != null) {
			String filterString = null;
			if (filter != null || innerDecorator != null) {
				filterString = (filter != null) ? filter.getFilterString()
						: innerDecorator.getFilterString();
			}
			StringBuffer tempBuffer = new StringBuffer()
					.append(super.getFilterString())
					.append(SPACE_STRING + outerOperator + SPACE_STRING)
					.append(filterString);
			if (isGrouped) {
				tempBuffer = new StringBuffer().append(STARTING_BRACE)
						.append(tempBuffer).append(ENDING_BRACE);
			}
			return tempBuffer.toString();
		}
		throw new QueryFormationException(
				"Error encountered while processing filters");
	}

	public FilterDecorator(Column column, LogicalOperator operator, String value) {
		super(column, operator, value);
	}
	
	public FilterDecorator(){
		
	}

}
