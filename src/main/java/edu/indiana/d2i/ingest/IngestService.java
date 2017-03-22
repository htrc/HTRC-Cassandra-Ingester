package edu.indiana.d2i.ingest;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.ingest.cassandra.CassandraAccessLevelUpdater;
import edu.indiana.d2i.ingest.cassandra.CassandraIngester;
import edu.indiana.d2i.ingest.cassandra.CassandraManager;
import edu.indiana.d2i.ingest.cassandra.MarcProcessor;
import edu.indiana.d2i.ingest.cassandra.CassandraPageTextIngester;
import edu.indiana.d2i.ingest.util.Configuration;
import edu.indiana.d2i.ingest.util.Tools;

public class IngestService {
	private static Logger log = LogManager.getLogger(IngestService.class);
	public static void main(String[] args) {
		log.info("load volume ids to ingest...");
		List<String> volumesToIngest = Tools.getVolumeIds(new File(Configuration.getProperty("VOLUME_ID_LIST")));
		CassandraIngester ingester = new CassandraIngester();
		ingester.addIngester(new CassandraPageTextIngester());
		log.info("page and zip ingest process starts");
		ingester.ingest(volumesToIngest);
		log.info("page and zip ingest process ends");
		
		log.info("update marc...");
		MarcProcessor marcProcessor = new MarcProcessor();
		marcProcessor.process(volumesToIngest);
		log.info("marc update ends");
		
		log.info("update access level ...");
		Updater accessLevelUpdater = new CassandraAccessLevelUpdater();
		accessLevelUpdater.update(volumesToIngest);
		log.info("access level update ends");
		CassandraManager.shutdown();
	}
}
