package edu.indiana.d2i.ingest.redis;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.Configuration;

/*
 * Class to set the availability status of volumes in Redis. This status indicates the availability of the volume in Cassandra. When a volume is 
 * ingested into (deleted from) Cassandra, its availability status is set to true (false). 
 */
public class RedisAvailStatusUpdater {
	private static final Logger logger = LogManager.getLogger(RedisAvailStatusUpdater.class);

	private RedisClient redisClient;
	private String redisAvailStatusHashFieldName;

	public RedisAvailStatusUpdater(RedisClient redisClient) {
		this.redisClient = redisClient;
		this.redisAvailStatusHashFieldName = Configuration.getProperty(Constants.PK_REDIS_AVAIL_STATUS_HASH_FIELD_NAME, Constants.DEFAULT_REDIS_AVAIL_STATUS_HASH_FIELD_NAME);
	}
	
	public boolean setAvailStatus(String volumeId, boolean availStatus) {
		String availability = (availStatus ? "true" : "false");
		boolean setResult = this.redisClient.setHashFieldValue(RedisRightsUtils.volumeIdToRedisKey(volumeId), this.redisAvailStatusHashFieldName, availability);
		if (setResult == false) {
			logger.error("Failed attempt to set of availability status of {} to {} in redis", volumeId, availStatus);
		}
		return setResult;
	}
	
	public List<String> setStatusToAvailable(List<String> volumeIds) {
		// TO DO
		return Collections.emptyList();
	}

	public List<String> setStatusToUnvailable(List<String> volumeIds) {
		// TO DO
		return Collections.emptyList();
	}
}
