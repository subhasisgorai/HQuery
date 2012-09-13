package org.hquery.querygen.visitor;

public interface QueryElement {
	public abstract void accept(QueryElementVisitor visitor);
}
