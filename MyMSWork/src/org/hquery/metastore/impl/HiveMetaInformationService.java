package org.hquery.metastore.impl;

import static org.hquery.common.util.HQueryConstants.COMMA;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;
import org.hquery.common.util.HQueryUtil;
import org.hquery.metastore.MetaInformationService;

public class HiveMetaInformationService implements MetaInformationService {
	private HiveMetaStoreClient metaStoreClient;
	private String dbName;

	public void setMetaStoreClient(HiveMetaStoreClient metaStoreClient) {
		this.metaStoreClient = metaStoreClient;
	}

	public HiveMetaInformationService(String dbName) {
		this.dbName = dbName;
	}

	@Override
	public List<String> listAllDatabases() {
		assert (metaStoreClient != null) : "Meta Store Client not intilalized";
		try {
			return metaStoreClient.getAllDatabases();
		} catch (MetaException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<String> listAllTables() {
		assert (metaStoreClient != null) : "Meta Store Client not intilalized";
		assert (StringUtils.isNotBlank(dbName)) : "DB name shouldn't be null";
		try {
			return metaStoreClient.getAllTables(dbName);
		} catch (MetaException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<FieldSchema> listFields(String tableName) {
		assert (metaStoreClient != null) : "Meta Store Client not intilalized";
		assert (StringUtils.isNotBlank(dbName)) : "DB name shouldn't be null";
		assert (StringUtils.isNotBlank(tableName)) : "Table name shouldn't be null";
		try {
			return metaStoreClient.getSchema(dbName, tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<Partition> listpartitions(String tableName, short max_parts)
			throws NoSuchObjectException, MetaException, TException {
		return metaStoreClient.listPartitions(dbName, tableName, max_parts);
	}

	@Override
	public List<String> listBuiltInFunctions() {
		return Arrays.asList(HQueryUtil.getResourceString("hquery-conf",
				"hive.builtin.functions").split(COMMA));
	}

	@Override
	public List<String> listBuiltInAggregateFunctions() {
		return Arrays.asList(HQueryUtil.getResourceString("hquery-conf",
				"hive.builtin.aggregate.functions").split(COMMA));
	}

	@Override
	public void init() {
		HiveConf conf = new HiveConf(HiveMetaInformationService.class);
		assert (conf != null) : "Hive Conf should not be null in order to use HiveConf";
		try {
			metaStoreClient = new HiveMetaStoreClient(conf);
		} catch (MetaException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void destroy() throws Exception {
		if (metaStoreClient != null) {
			System.out.println("Now shutting down metaStoreClient");
			metaStoreClient.close();
		}
	}

}
