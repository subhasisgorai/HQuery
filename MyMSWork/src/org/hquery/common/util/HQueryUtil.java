package org.hquery.common.util;

import java.util.Formatter;
import java.util.Locale;
import java.util.ResourceBundle;

public class HQueryUtil {
	public static String getResourceString(String resourceFile, String key) {
		Locale defaultLocale = Locale.getDefault();
		return getResourceString(resourceFile, key, defaultLocale);
	}

	public static String getResourceString(String resourceFile, String key,
			Locale locale) {
		ResourceBundle myResources = ResourceBundle.getBundle(resourceFile,
				locale);
		return myResources.getString(key);

	}

	public static String getDelimiterTranslationCommand(String hdfsFile,
			String delimiter, String userFile) {
		StringBuffer sb = new StringBuffer();
		Formatter formatter = new Formatter(sb);
		formatter.format("/Users/subhasig/MyWorkspace/hadoop-1.0.3/bin/hadoop dfs -cat %s | tr \"\\001\" %s > %s", hdfsFile,
				delimiter, userFile);
		formatter.flush();
		formatter.close();
		return sb.toString();
	}

}