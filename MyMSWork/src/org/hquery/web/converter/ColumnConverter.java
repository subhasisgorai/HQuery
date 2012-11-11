package org.hquery.web.converter;

import javax.faces.component.UIComponent;
import javax.faces.context.FacesContext;
import javax.faces.convert.Converter;

import org.hquery.web.Column;

public class ColumnConverter implements Converter {

	@Override
	public Object getAsObject(FacesContext context, UIComponent component,
			String value) {
		if (value != null) {
			String[] tokens = value.split("\\.");
			if (tokens != null && tokens.length == 2)
				return new Column(tokens[1], tokens[0]);
		}
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
