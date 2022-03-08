package edu.indiana.d2i.ingest.cassandra;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import com.datastax.oss.driver.api.core.cql.ResultSet;

//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Row;
//import com.datastax.driver.core.Statement;
//import com.datastax.driver.core.querybuilder.QueryBuilder;
//import com.datastax.driver.core.querybuilder.Select.Selection;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class CassandraScanner {

/*	public static void main(String[] args) throws FileNotFoundException {
		CassandraManager cassandraManager = CassandraManager.getInstance();
		Selection selectBuilder = QueryBuilder.select();
		Statement stmt = null;
		if(args[0].equals("pd_table")) {
			stmt = selectBuilder.distinct().column("volumeid").from("htrcpdcorpus", "pdvolumecontents");
		} else if(args[0].equals("full_corpus")) {
			stmt = selectBuilder.distinct().column("volumeid").from("htrccorpus", "volumecontents");
		} else {
			System.out.println("error: unrecoginized input. please enter pd_table for public domain or full_corpus for all volumes");
			return;
		}
		
		ResultSet resultSet = cassandraManager.execute(stmt.setFetchSize(100));
		
		PrintWriter pw = new PrintWriter("result-keys.txt");
		for(Row row : resultSet) {
			String id = row.getString("volumeid");
			pw.println(id); pw.flush();
		}
		pw.flush(); pw.close();
		System.out.println("scan finished.");
		CassandraManager.shutdown();
	}
*/
	public static void main(String[] args) throws FileNotFoundException {
		CassandraManager cassandraManager = CassandraManager.getInstance();
		String selectStr;
		
		if (args[0].equals("pd_table")) {
			selectStr = "SELECT DISTINCT volumeid FROM htrcpdcorpus.pdvolumecontents";
		} else if (args[0].equals("full_corpus")) {
			selectStr = "SELECT DISTINCT volumeid FROM htrccorpus.volumecontents";
		} else {
			System.out.println("error: unrecoginized input. please enter pd_table for public domain or full_corpus for all volumes");
			return;
		}
		
		PrintWriter pw = new PrintWriter("result-keys.txt");
		try {
			ResultSet resultSet = cassandraManager.executeWithoutRetry(SimpleStatement.newInstance(selectStr).setPageSize(100));
			for (Row row : resultSet) {
				String id = row.getString("volumeid");
				pw.println(id); pw.flush();
			}
		} catch (Exception e) {
			System.out.println("Execution failed: " + selectStr);
			e.printStackTrace();
		}
		
		pw.flush(); pw.close();
		System.out.println("scan finished.");
		CassandraManager.shutdown();
	}
	
}
