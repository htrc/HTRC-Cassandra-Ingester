package edu.indiana.d2i.ingest;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.ingest.cassandra.CassandraIngester;
import edu.indiana.d2i.ingest.util.Configuration;
import edu.indiana.d2i.ingest.util.Tools;

public class IngestService {
	private static Logger log = LogManager.getLogger(IngestService.class);
	public static void main(String[] args) {
		log.info("load volume ids to ingest...");
		List<String> volumesToIngest = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
		Ingester ingester = new CassandraIngester();
		log.info("ingest process starts");
		ingester.ingest(volumesToIngest);
		log.info("ingest process ends");
	}
}
