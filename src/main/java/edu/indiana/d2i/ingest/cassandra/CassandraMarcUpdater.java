package edu.indiana.d2i.ingest.cassandra;

import java.util.List;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.Updater;
import edu.indiana.d2i.ingest.util.Configuration;


public class CassandraMarcUpdater extends Updater{
	private CassandraManager cassandraManager;
	String columnFamilyName;
	public CassandraMarcUpdater () {
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY);
	}
	@Override
	public boolean update(List<String> volumeIds) {
		// to be implemented
		//1. get marc string
		
		//2. 
	//	Update updateQuery = QueryBuilder.update(columnFamilyName).with(QueryBuilder.set("marc", marcString))
	//			.where(QueryBuilder.eq("id", volumeId));
	
		return false;
	}

	
}
