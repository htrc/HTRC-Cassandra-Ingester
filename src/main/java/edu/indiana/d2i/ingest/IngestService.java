package edu.indiana.d2i.ingest;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.ingest.cassandra.CassandraAccessLevelUpdater;
import edu.indiana.d2i.ingest.cassandra.CassandraIngester;
import edu.indiana.d2i.ingest.cassandra.CassandraManager;
import edu.indiana.d2i.ingest.cassandra.MarcProcessor;
import edu.indiana.d2i.ingest.redis.RedisClient;
import edu.indiana.d2i.ingest.cassandra.CassandraPageTextIngester;
import edu.indiana.d2i.ingest.solr.SolrMetadtaIngester;
import edu.indiana.d2i.ingest.util.Configuration;
import edu.indiana.d2i.ingest.util.Tools;

public class IngestService {
	private static Logger log = LogManager.getLogger(IngestService.class);
	public static void main(String[] args) {
		log.info("load volume ids to ingest...");
		List<String> volumesToIngest = Tools.getVolumeIds(new File(
				Configuration.getProperty("VOLUME_ID_LIST")));

		RedisClient redisClient = null;
		
		if (Boolean.valueOf(Configuration.getProperty("PUSH_TO_CASSANDRA"))) {
			CassandraIngester ingester = new CassandraIngester();
			ingester.addIngester(new CassandraPageTextIngester());
			log.info("page and zip ingest process starts");
			ingester.ingest(volumesToIngest);
			log.info("page and zip ingest process ends");
		}
		
		if (Boolean.valueOf(Configuration.getProperty("PUSH_TO_SOLR"))) {
			log.info("ingest metadata to solr...");
			Ingester solrIngester = new SolrMetadtaIngester();
			// For the ids of volumes successfully pushed into Cassandra, ingest
			// their metadata into solr
			// so this one does not take volumesToIngest in order for
			// consistency with cassandra
			List<String> successIngested = Tools.getVolumeIds(new File(
					Configuration.getProperty("CASSANDRA_INGESTER_SUCCESS")));
			solrIngester.ingest(successIngested);
			log.info("metadata ingest ends");
		}

		if (Boolean.valueOf(Configuration
				.getProperty("UPDATE_MARC_TO_CASSANDRA"))) {
			log.info("update marc...");
			MarcProcessor marcProcessor = new MarcProcessor();
			marcProcessor.process(volumesToIngest);
			log.info("marc update ends");

			log.info("update access level ...");
			if (redisClient == null) {
				redisClient = new RedisClient();
			}												
			Updater accessLevelUpdater = new CassandraAccessLevelUpdater(redisClient);
			accessLevelUpdater.update(volumesToIngest);
			log.info("access level update ends");
		}
		CassandraManager.shutdown();
	}
}
