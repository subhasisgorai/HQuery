package org.hquery.web.converter;

import javax.faces.component.UIComponent;
import javax.faces.context.FacesContext;
import javax.faces.convert.EnumConverter;

import org.hquery.querygen.dbobjects.LogicalOperator;

public class OperatorConverter extends EnumConverter {

	public OperatorConverter() {
		super(OperatorConverter.class);
	}

	@Override
	public Object getAsObject(FacesContext context, UIComponent component,
			String value) {
		if (value != null)
			return LogicalOperator.fromString(value);
		return null;
	}

	@Override
	public String getAsString(FacesContext context, UIComponent component,
			Object value) {
		if (value != null)
			return value.toString();
		return null;
	}

}
