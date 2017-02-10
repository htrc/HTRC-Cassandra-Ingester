package edu.indiana.d2i.ingest;

import java.util.List;

public abstract class Ingester {

	public void ingest(List<String> volumes) {
		for(String id : volumes) {
			ingestOne(id);
		}
	}
	public abstract boolean ingestOne(String volumeId);
	
}
