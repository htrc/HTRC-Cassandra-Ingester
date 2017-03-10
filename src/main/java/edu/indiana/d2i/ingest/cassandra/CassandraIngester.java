package edu.indiana.d2i.ingest.cassandra;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

import edu.indiana.d2i.ingest.Ingester;

public class CassandraIngester extends Ingester{
	private static PrintWriter pw1;
	private static PrintWriter pw2;
	private List<Ingester> ingestersInOrder;
	static {
		try {
			pw1 = new PrintWriter("success.txt");
			pw2 = new PrintWriter("failure.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	public CassandraIngester() {
		ingestersInOrder = new LinkedList<Ingester>();
	}
	public void addIngester(Ingester ingester) {
		ingestersInOrder.add(ingester);
	}

	public boolean ingestOne(String volumeId) {
		boolean ingested = true;
		for(Ingester ingester : ingestersInOrder) {
			boolean flag = ingester.ingestOne(volumeId);
			ingested = flag && ingested;
		}
		if(ingested) {
			pw1.println(volumeId);pw1.flush();
		} else {
			pw2.println(volumeId);pw2.flush();
		}
		return ingested;
	}
}
