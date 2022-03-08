package edu.indiana.d2i.ingest;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.ingest.cassandra.CassandraIngester;
import edu.indiana.d2i.ingest.cassandra.CassandraManager;
import edu.indiana.d2i.ingest.cassandra.MarcProcessor;
import edu.indiana.d2i.ingest.redis.RedisAvailStatusUpdater;
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

		RedisClient redisClient = null; // use the same instance of RedisClient for CassandraAccessLevelUpdater, RedisAvailStatusUpdater
		
		if (Boolean.valueOf(Configuration.getProperty("PUSH_TO_CASSANDRA"))) {
			CassandraIngester ingester = new CassandraIngester();
			
			// pass accessLevelUpdater as argument to CassandraPageTextIngester;
			// call accessLevelUpdater.update(volumeId) after ingest of
			// volumeId
		//	ingester.addIngester(new CassandraPageTextIngester(accessLevelUpdater));
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
		}
		
		// the access level column is no longer present in the table
/*		if(Boolean.valueOf(Configuration.getProperty("UPDATE_ACCESS_LEVEL_TO_CASSANDRA"))) {
			log.info("update access level...");
			if (redisClient == null) {
				redisClient = new RedisClient();
			}
			Updater accessLevelUpdater = new CassandraAccessLevelUpdater(redisClient);
			List<String> cassandraIngestedVolumes = Tools.getVolumeIds(new File(
					Configuration.getProperty("CASSANDRA_INGESTER_SUCCESS")));
			List<String> updatSuccessList = accessLevelUpdater.update(cassandraIngestedVolumes);
			try {
				Tools.generateVolumeListFile(Configuration.getProperty("ACCESS_LEVEL_UPDATE_FAILURE_LIST"), updatSuccessList);
			} catch (FileNotFoundException e) {
				log.error("access level update output file exception", e);
			}
			log.info("access level update ends ...");
		}
*/
		if (Boolean.valueOf(Configuration.getProperty("UPDATE_AVAIL_STATUS_IN_REDIS"))) {
			log.info("update availability status in redis ...");
			if (redisClient == null) {
				redisClient = new RedisClient();
			}
			RedisAvailStatusUpdater updater = new RedisAvailStatusUpdater(redisClient);
			
			List<String> cassandraIngestedVolumes = Tools.getVolumeIds(new File(
					Configuration.getProperty("CASSANDRA_INGESTER_SUCCESS")));
			updater.setStatusToAvailable(cassandraIngestedVolumes);
			
			// use the following for volumes deleted from Cassandra
			// updater.setStatusToUnavailable(volumesToDelete);
			
			log.info("availability status update ends ...");
		}
				
		CassandraManager.shutdown();
	}
}
