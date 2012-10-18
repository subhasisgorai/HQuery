package org.hquery.web;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.hquery.assembler.HQueryAssembler;
import org.hquery.metastore.MetaInformationService;

public class HQueryController {
	private HQueryAssembler assembler;

	public HQueryAssembler getAssembler() {
		return assembler;
	}

	public void setAssembler(HQueryAssembler assembler) {
		this.assembler = assembler;
	}

	public String[] selectedTables;
	public String[] selectedColumns;
	public Map<String, List<String>> tableColumnsMap = new HashMap<String, List<String>>();

	public String[] getSelectedTables() {
		System.out.println("####### selected tables called ####");
		return selectedTables;
	}

	public void setSelectedTables(String[] selectedTables) {
		this.selectedTables = selectedTables;
	}

	public String[] getSelectedColumns() {
		return selectedColumns;
	}

	public void setSelectedColumns(String[] selectedColumns) {
		this.selectedColumns = selectedColumns;
	}

	public String[] getAllTables() {
		MetaInformationService metaService = assembler
				.getMetaInformationService();
		List<String> tableList = metaService.listAllTables();
		String[] tables = new String[tableList.size()];
		tables = tableList.toArray(tables);
		return tables;
	}

	public List<String> getAllColumns() {
		MetaInformationService metaService = assembler
				.getMetaInformationService();
		List<String> columns = null;
		String[] selectedTables = getSelectedTables();
		if (selectedTables != null && selectedTables.length > 0) {
			columns = new ArrayList<String>();
			for (String table : selectedTables) {
				if (tableColumnsMap.containsKey(table)) {
					columns.addAll(tableColumnsMap.get(table));
				} else {
					List<String> tempColumns = new ArrayList<String>();
					List<FieldSchema> fields = metaService.listFields(table);
					for (FieldSchema field : fields) {
						tempColumns.add(field.getName());
					}
					tableColumnsMap.put(table, tempColumns);
					columns.addAll(tableColumnsMap.get(table));
				}
			}
		}
		return columns;
	}

}
