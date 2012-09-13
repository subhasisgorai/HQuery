package org.hquery.metastore;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;
import org.springframework.beans.factory.DisposableBean;

public interface MetaInformationService extends DisposableBean{
	public List<String> listAllDatabases();

	public List<String> listAllTables();

	public List<FieldSchema> listFields(String tableName);

	public List<Partition> listpartitions(String tableName,
			short max_parts) throws NoSuchObjectException, MetaException, TException;

	public List<String> listBuiltInFunctions();
	
	public List<String> listBuiltInAggregateFunctions();
	
	public void init();
	
	public void destroy() throws Exception;
}
