package org.hquery.querygen.dialect;

import java.util.Formatter;

import org.apache.commons.lang.StringUtils;

public class HiveDialect {

	public static String getSelectQuery(boolean isDistinct, String... args) {
		String projectedColumnsString = args[0];
		String fromTablesString = args[1];
		String groupByColumnsString = args[2];
		String filterString = args[3];
		String distinctString = (isDistinct) ? "distinct " : "";
		assert (!StringUtils.isBlank(projectedColumnsString)) : "Projected columns list can't be NULL";
		assert (!StringUtils.isBlank(fromTablesString)) : "Tables list can't be NULL";
		return new StringBuffer()
				.append("select ")
				.append(distinctString)
				.append(projectedColumnsString)
				.append(" from ")
				.append(fromTablesString)
				.append(((!StringUtils.isBlank(filterString)) ? " where "
						+ filterString : ""))
				.append((!StringUtils.isBlank(groupByColumnsString)) ? " group by "
						+ groupByColumnsString
						: "").toString();
	}

	public static StringBuffer getInsertOverwriteString(String fileLocation) {
		StringBuffer sb = new StringBuffer();
		Formatter formatter = new Formatter(sb);
		formatter.format("insert overwrite directory '%s'", fileLocation);
		formatter.flush();
		formatter.close();
		return sb;
	}

}
