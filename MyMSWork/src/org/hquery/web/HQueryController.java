package org.hquery.web;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.faces.component.UIData;
import javax.faces.event.ActionEvent;
import javax.faces.model.SelectItem;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.mapred.JobStatus;
import org.hquery.assembler.HQueryAssembler;
import org.hquery.common.util.UserPreferences;
import org.hquery.common.util.UserPreferences.FileType;
import org.hquery.metastore.MetaInformationService;
import org.hquery.querygen.dbobjects.Column.DataType;
import org.hquery.querygen.dbobjects.LogicalOperator;
import org.hquery.querygen.dbobjects.Table;
import org.hquery.querygen.filter.decorator.FilterDecorator;
import org.hquery.querygen.query.Query;
import org.hquery.querygen.query.QueryType;
import org.hquery.status.StatusChecker;
import org.hquery.status.impl.JobStatusCheckerImpl.StatusEnum;

public class HQueryController {
	private HQueryAssembler assembler;

	public HQueryAssembler getAssembler() {
		return assembler;
	}

	public void setAssembler(HQueryAssembler assembler) {
		this.assembler = assembler;
	}

	public String[] selectedTables;
	public List<Column> selectedColumns;
	public Map<String, List<SelectItem>> tableColumnsMap = new HashMap<String, List<SelectItem>>();

	public String[] getSelectedTables() {
		return selectedTables;
	}

	public void setSelectedTables(String[] selectedTables) {
		this.selectedTables = selectedTables;
	}

	public List<Column> getSelectedColumns() {
		return selectedColumns;
	}

	public void setSelectedColumns(List<Column> selectedColumns) {
		this.selectedColumns = selectedColumns;
	}

	String[] tables = null;

	public String[] getAllTables() {
		MetaInformationService metaService = assembler
				.getMetaInformationService();
		List<String> tableList = metaService.listAllTables();
		tables = new String[tableList.size()];
		tables = tableList.toArray(tables);
		return tables;
	}

	private List<SelectItem> columns;

	public List<SelectItem> getColumns() {
		String[] selectedTables = getSelectedTables();
		if (selectedTables != null && selectedTables.length > 0) {
			columns = new ArrayList<SelectItem>();
			for (String table : selectedTables) {
				if (tableColumnsMap.containsKey(table)) {
					columns.addAll(tableColumnsMap.get(table));
				} else {
					List<SelectItem> tempColumns = new ArrayList<SelectItem>();
					MetaInformationService metaService = assembler
							.getMetaInformationService();
					List<FieldSchema> fields = metaService.listFields(table);
					for (FieldSchema field : fields) {
						tempColumns.add(new SelectItem(new Column(field
								.getName(), table, field.getType()), field
								.getName() + " [" + table + "]"));
					}
					tableColumnsMap.put(table, tempColumns);
					columns.addAll(tableColumnsMap.get(table));
				}
			}
		} else
			columns = null;
		return columns;
	}

	public void setColumns(List<SelectItem> columns) {
		this.columns = columns;
	}

	public String addFilter() {
		if (filters == null)
			filters = new ArrayList<Filter>();
		Filter filter = new Filter();
		filter.setColumn(filteredColumn);
		filter.setOuterOperator(LogicalOperator.fromString(outerOperator));
		filter.setInnerOperator(LogicalOperator.fromString(innerOperator));
		filter.setValue(value);
		filters.add(filter);
		return null;
	}

	public String deleteFilter() {
		filters.remove(table.getRowData());
		return null;
	}

	private List<Filter> filters;

	public List<Filter> getFilters() {
		return filters;
	}

	public void setFilters(List<Filter> filters) {
		this.filters = filters;
	}

	public boolean isRenderFilterTable() {
		return filters != null && filters.size() > 0;
	}

	public boolean isOuterOpDisabled() {
		return filters == null || filters.size() == 0;
	}

	private UIData table;

	public UIData getTable() {
		return table;
	}

	public void setTable(UIData table) {
		this.table = table;
	}

	private String outerOperator;
	private Column filteredColumn;
	private String innerOperator;
	private String value;

	public String getOuterOperator() {
		return outerOperator;
	}

	public void setOuterOperator(String outerOperator) {
		this.outerOperator = outerOperator;
	}

	public Column getFilteredColumn() {
		return filteredColumn;
	}

	public void setFilteredColumn(Column filteredColumn) {
		this.filteredColumn = filteredColumn;
	}

	public String getInnerOperator() {
		return innerOperator;
	}

	public void setInnerOperator(String innerOperator) {
		this.innerOperator = innerOperator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public HQueryController() {

		innerOperators = new ArrayList<String>();
		innerOperators.add(LogicalOperator.EQ.toString());
		innerOperators.add(LogicalOperator.GE.toString());
		innerOperators.add(LogicalOperator.GT.toString());
		innerOperators.add(LogicalOperator.LE.toString());
		innerOperators.add(LogicalOperator.LT.toString());
		innerOperators.add(LogicalOperator.NOT_EQ.toString());

		outerOpertaors = new ArrayList<String>();
		outerOpertaors.add(LogicalOperator.AND.toString());
		outerOpertaors.add(LogicalOperator.OR.toString());

	}

	private List<String> innerOperators = null;
	private List<String> outerOpertaors = null;

	public List<String> getInnerOperators() {
		return this.innerOperators;
	}

	public List<String> getOuterOpertaors() {
		return this.outerOpertaors;
	}

	private List<Column> groupByColumns;

	public List<Column> getGroupByColumns() {
		return groupByColumns;
	}

	public void setGroupByColumns(List<Column> groupByColumns) {
		this.groupByColumns = groupByColumns;
	}

	private List<ColumnFunction> columnFunctions;

	public List<ColumnFunction> getColumnFunctions() {
		return columnFunctions;
	}

	public void setColumnFunctions(List<ColumnFunction> columnFunctions) {
		this.columnFunctions = columnFunctions;
	}

	private UIData functionsTable;

	public UIData getFunctionsTable() {
		return functionsTable;
	}

	public void setFunctionsTable(UIData functionsTable) {
		this.functionsTable = functionsTable;
	}

	public String addFunction() {
		if (columnFunctions == null)
			columnFunctions = new ArrayList<ColumnFunction>();
		columnFunctions.add(new ColumnFunction(null, ""));
		return null;
	}

	public List<String> getFunctionStrings() {
		MetaInformationService metaService = assembler
				.getMetaInformationService();
		List<String> functions = new ArrayList<String>();
		functions.add("");
		functions.addAll(metaService.listBuiltInAggregateFunctions());
		functions.addAll(metaService.listBuiltInAggregateFunctions());
		return functions;
	}

	public String deleteFunction() {
		columnFunctions.remove(functionsTable.getRowData());
		return null;
	}

	public boolean isRenderFunctionTable() {
		return columnFunctions != null && columnFunctions.size() > 0;
	}

	public void processQuery(ActionEvent event) {
		Query query = new Query();
		if (selectedTables != null && selectedTables.length > 0
				&& tableColumnsMap != null) {
			String tableName = selectedTables[0]; // need to fix later when we support joins from UI
			Table table = new Table(tableName);

			//processing projected columns and functions
			if (!CollectionUtils.isEmpty(selectedColumns))
				for (Column selectedColumn : selectedColumns) {
					org.hquery.querygen.dbobjects.Column queryGenColumn = getQueryGenColumn(
							table, selectedColumn);

					if (!CollectionUtils.isEmpty(columnFunctions)) {
						for (ColumnFunction columnFunction : columnFunctions) {
							if (columnFunction.getColumn().equals(
									selectedColumn)) {
								queryGenColumn.setFunctionName(columnFunction
										.getFunctionName());
								break;
							}
						}
					}
					table.addProjectedColumn(queryGenColumn);
				}

			//processing group by
			if (!CollectionUtils.isEmpty(groupByColumns)) {
				for (Column groupByColumn : groupByColumns) {
					table.addGroupByColumn(getQueryGenColumn(table,
							groupByColumn));
				}
			}

			//processing filters
			org.hquery.querygen.filter.Filter queryGenFilter = null;
			if (!CollectionUtils.isEmpty(filters)) {
				Iterator<Filter> filterIterator = filters.iterator();
				queryGenFilter = transformFilter(filterIterator.next(), table);
				while (filterIterator.hasNext()) {
					FilterDecorator filterDecorator = (FilterDecorator) transformFilter(
							filterIterator.next(), table);
					filterDecorator.setFilter(queryGenFilter);
					queryGenFilter = filterDecorator;
				}

			}
			table.setFilter(queryGenFilter);
			query.setTable(table);
			query.setQueryType(QueryType.SELECT_QUERY); //hard-coded for the time being
		}

		//processing user preferences
		UserPreferences pref = new UserPreferences();
		pref.setOutputFileType(FileType.CSV_TYPE);
		pref.setOutputFile("/Users/subhasig/test.txt");

		//now execute the query
		this.sessionId = assembler.executeQuery(query, pref);
		System.out.println("Session Id: " + this.sessionId);

		progressMonitorActivated = "true";

		//not required for this web application
		//		try {
		//			Thread.sleep(Long.parseLong(HQueryUtil.getResourceString(
		//					"hquery-conf", "hquery.cooldown.period"))); //giving other daemon thread a chance to graceful stop 
		//		} catch (NumberFormatException e) {
		//			e.printStackTrace();
		//		} catch (InterruptedException e) {
		//			e.printStackTrace();
		//		}

	}

	private org.hquery.querygen.filter.Filter transformFilter(Filter filter,
			Table table) {
		org.hquery.querygen.filter.Filter queryGenFilter = null;
		if (filter.getOuterOperator() != null) {
			queryGenFilter = new FilterDecorator();
			((FilterDecorator) queryGenFilter).setOuterOperator(filter
					.getOuterOperator());
		} else {
			queryGenFilter = new org.hquery.querygen.filter.Filter();
		}
		queryGenFilter.setColumn(getQueryGenColumn(table, filter.getColumn()));
		queryGenFilter.setOperator(filter.getInnerOperator());
		queryGenFilter.setValue(filter.getValue());
		return queryGenFilter;
	}

	private Map<String, org.hquery.querygen.dbobjects.Column> columnMap = new HashMap<String, org.hquery.querygen.dbobjects.Column>();

	public org.hquery.querygen.dbobjects.Column getQueryGenColumn(Table table,
			Column selectedColumn) {
		if (columnMap.containsKey(table.getTableName() + "."
				+ selectedColumn.getName()))
			return columnMap.get(table.getTableName() + "."
					+ selectedColumn.getName());
		else
			for (SelectItem item : tableColumnsMap.get(table.getTableName())) {
				Column column = (Column) item.getValue();
				if (column.equals(selectedColumn)) {
					org.hquery.querygen.dbobjects.Column queryGenColumn = new org.hquery.querygen.dbobjects.Column();
					queryGenColumn.setColumnName(column.getName());
					queryGenColumn.setOwningTable(table);
					queryGenColumn.setDataType(DataType.valueOf(column
							.getType().toUpperCase()));
					columnMap.put(
							table.getTableName() + "."
									+ selectedColumn.getName(), queryGenColumn);
					return queryGenColumn;
				}
			}
		return null;
	}

	private String sessionId;
	private String progressMonitorActivated;

	public String getProgressMonitorActivated() {
		return progressMonitorActivated;
	}

	public void setProgressMonitorActivated(String progressMonitorActivated) {
		this.progressMonitorActivated = progressMonitorActivated;
	}

	public List<JobStatus> getStatus(String sessionId) {
		assert (StringUtils.isNotBlank(sessionId)) : "Session id shouldn't be null for getting status";
		StatusChecker statusChecker = assembler.getStatusChecker();
		return statusChecker.getStatus(sessionId);
	}

	private Map<String, Integer> statusMap = new HashMap<String, Integer>();

	public void checkStatus(ActionEvent event) {
		StatusChecker statusChecker = assembler.getStatusChecker();
		StringBuilder sb = new StringBuilder();
		if (StringUtils.isBlank(statusString)) {
			sb.append("\n");
			sb.append("Job Progress Status: ");
		}
		StatusEnum overallStatus = statusChecker.checkStatus(sessionId);
		if (overallStatus == StatusEnum.UNKNOWN) {
			sb.append("\nUnknown ");
		} else if (overallStatus == StatusEnum.COMPLETED) {
			sb.append("\nCompleted ");
			progressMonitorActivated = "false";
		} else {
			List<JobStatus> statuses = getStatus(sessionId);
			if (statuses != null && !statuses.isEmpty()) { //iterate through the statuses of the jobs belongs a particular session
				int prepCounter = 0;
				int runningCounter = 0;
				for (JobStatus status : statuses) {
					if (status.getRunState() == JobStatus.RUNNING)
						runningCounter++;
					else if (status.getRunState() == JobStatus.PREP)
						prepCounter++;
				}
				if ((prepCounter == statuses.size())
						&& (statusMap.get(sessionId) == null || statusMap.get(
								sessionId).intValue() != JobStatus.PREP)) {
					statusMap.put(sessionId, JobStatus.PREP);
					sb.append("\nPreparing ");
				} else if ((runningCounter > 0)
						&& (statusMap.get(sessionId) == null || statusMap.get(
								sessionId).intValue() != JobStatus.RUNNING)) {
					statusMap.put(sessionId, JobStatus.RUNNING);
					sb.append("\nRunning\n ");
				} else if ((runningCounter > 0)
						&& (statusMap.get(sessionId) == null || statusMap.get(
								sessionId).intValue() == JobStatus.RUNNING)) {
					for (JobStatus status : statuses) {
						StringBuffer runningBuffer = new StringBuffer();
						Formatter formatter = new Formatter(runningBuffer);
						formatter
								.format("\t[Job Id %d] Map progress: %.1f%%, Reduce Progress: %.1f%%%n",
										status.getJobID().getId(),
										status.mapProgress() * 100,
										status.reduceProgress() * 100);
						formatter.flush();
						formatter.close();
						sb.append(runningBuffer);
					}
				} else {
					sb.append("..");
				}
			}
		}
		statusString += sb.toString();
	}

	private String statusString;

	public String getStatusString() {
		return statusString;
	}

	public void setStatusString(String statusString) {
		this.statusString = statusString;
	}

}
