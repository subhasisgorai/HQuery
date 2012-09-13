package org.hquery.querygen.visitor;

import org.hquery.querygen.dbobjects.Column;
import org.hquery.querygen.dbobjects.Table;

public interface QueryElementVisitor {
	public void visit(Table table);

	public void visit(Column column);

	public void setProcessingProjectedColumnsOver(
			boolean processingProjectedColumns);

}
