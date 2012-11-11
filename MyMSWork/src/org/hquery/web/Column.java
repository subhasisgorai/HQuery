package org.hquery.web;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class Column {
	private String name;
	private String owningTable;

	public Column(String name, String owningTable) {
		this.name = name;
		this.owningTable = owningTable;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOwningTable() {
		return owningTable;
	}

	public void setOwningTable(String owningTable) {
		this.owningTable = owningTable;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Column == false) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		final Column otherObject = (Column) obj;
		return new EqualsBuilder().append(this.name, otherObject.getName())
				.append(this.owningTable, otherObject.getOwningTable())
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37).append(name).append(owningTable)
				.toHashCode();
	}

	@Override
	public String toString() {
		return owningTable + "." + name;
	}

}
