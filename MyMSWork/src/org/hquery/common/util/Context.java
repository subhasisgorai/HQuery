package org.hquery.common.util;

import java.util.HashMap;
import java.util.Map;

public class Context {
	Map<String, Object> map = new HashMap<String, Object>();

	public Context putInContext(String key, Object object) {
		this.map.put(key, object);
		return this;
	}

	public Object get(String key) {
		return map.get(key);
	}
}
