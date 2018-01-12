package edu.indiana.d2i.delete;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.cassandra.CassandraManager;
import edu.indiana.d2i.ingest.util.Configuration;
import edu.indiana.d2i.ingest.util.Tools;

public class CassandraVolumeRemover {

	public static void main(String[] args) throws FileNotFoundException {
		// args[0] is the id list file path that will be removed 
		//args[1] is the file path of successfully removed ids
		removeFromCassandra(args[0], args[1]);

	}
	
	private static void removeFromCassandra(String idListFilePath, String outputFilePath) throws FileNotFoundException {
		List<String> volumeIdsToRemove = Tools.getVolumeIds(new File(idListFilePath));
		CassandraManager cassandraManager = CassandraManager.getInstance();
		//PrintWriter pw = new PrintWriter(outputFilePath);
		for(String volumeid : volumeIdsToRemove) {
		//	System.out.println("remove " + volumeid);
			Delete.Where query = QueryBuilder.delete().all().from(Configuration.getProperty("KEY_SPACE"), Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY))
			.where(QueryBuilder.eq("volumeid", volumeid));
		//	System.out.println("delete query: " + query.toString());
			cassandraManager.execute(query);
		}
	//	pw.flush(); pw.close();	
		CassandraManager.shutdown();
	}

}
