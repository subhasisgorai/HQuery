package org.hquery.web;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.faces.component.UIData;
import javax.faces.event.ActionEvent;
import javax.faces.model.SelectItem;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.hquery.assembler.HQueryAssembler;
import org.hquery.metastore.MetaInformationService;
import org.hquery.querygen.dbobjects.LogicalOperator;

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
		MetaInformationService metaService = assembler
				.getMetaInformationService();
		String[] selectedTables = getSelectedTables();
		if (selectedTables != null && selectedTables.length > 0) {
			columns = new ArrayList<SelectItem>();
			for (String table : selectedTables) {
				if (tableColumnsMap.containsKey(table)) {
					columns.addAll(tableColumnsMap.get(table));
				} else {
					List<SelectItem> tempColumns = new ArrayList<SelectItem>();
					List<FieldSchema> fields = metaService.listFields(table);
					for (FieldSchema field : fields) {
						tempColumns.add(new SelectItem(new Column(field
								.getName(), table), field.getName() + " ["
								+ table + "]"));
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

	public void processQuery(ActionEvent event) {
		System.out.println(selectedColumns);

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

}
