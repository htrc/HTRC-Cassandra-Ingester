package edu.indiana.d2i.ingest.cassandra;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Iterator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;

public class CassandraScanner {

	public static void main(String[] args) throws FileNotFoundException {
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
	}

}
