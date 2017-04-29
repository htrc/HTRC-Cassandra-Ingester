package edu.indiana.d2i.ingest.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.Updater;
import edu.indiana.d2i.ingest.redis.RedisClient;
import edu.indiana.d2i.ingest.redis.RedisRightsUtils;
import edu.indiana.d2i.ingest.util.Configuration;

/*
 * Class to update the access level column in Cassandra. At this time, access level information is obtained from the Redis database that maintains
 * the same.
 */
public class CassandraAccessLevelUpdater extends Updater {
	private static final Logger logger = LogManager.getLogger(CassandraAccessLevelUpdater.class);

	private RedisClient redisClient;
	private SimpleColumnIngester<Integer> accessLevelIngester;
	private String redisAccessLevelFieldName;
	
	public CassandraAccessLevelUpdater(RedisClient redisClient) {
		super();
		this.redisClient = redisClient;
		String csdAccessLevelCol = Configuration.getProperty(Constants.PK_ACCESS_LEVEL_COLUMN, Constants.DEFAULT_ACCESS_LEVEL_COLUMN);
		this.accessLevelIngester = new SimpleColumnIngester<Integer>(csdAccessLevelCol);
		this.redisAccessLevelFieldName = Configuration.getProperty(Constants.PK_REDIS_ACCESS_LEVEL_HASH_FIELD_NAME, Constants.DEFAULT_REDIS_ACCESS_LEVEL_HASH_FIELD_NAME);
	}

	// update the access level column for the given volume id in Cassandra, after first obtaining the access level of the volume from Redis
	public boolean update(String volumeId) {
		// get the access level of the volume from redis
		String accessLevel = redisClient.getHashFieldValue(RedisRightsUtils.volumeIdToRedisKey(volumeId), this.redisAccessLevelFieldName);
		// set the access level in Cassandra
		return this.updateAccessLevel(volumeId, accessLevel);
	}
	
	// update the access level column for the given volume ids in Cassandra, after first obtaining the access levels of the volumes from Redis
	// result: the list of volume ids for which the update of the access level column failed
	@Override
	public List<String> update(List<String> volumeIds) {
		// get the access levels of the volumes from redis
		List<Map.Entry<String, String>> volIdLevels = 
				redisClient.getHashFieldValues(volumeIds, this.redisAccessLevelFieldName, RedisRightsUtils::volumeIdToRedisKey);

		// set the access levels in Cassandra
		List<String> failedUpdateVolIds = new ArrayList<String>();
		// use for loop instead of streams because lambda expressions for the latter will most likely have side-effects
		for (Map.Entry<String, String> entry : volIdLevels) {
			if (! this.updateAccessLevel(entry.getKey(), entry.getValue())) {
				failedUpdateVolIds.add(entry.getKey());
			}
		}
		return failedUpdateVolIds;
	}

	// given a volume id and access level, set the value of the access level column in Cassandra to the abbreviated, integer access level
	//  result: true if the update is successful, false otherwise
	private boolean updateAccessLevel(String volumeId, String accessLevel) {
		int abbrevLevel = RedisRightsUtils.abbrevAccessLevel(accessLevel);
		if (abbrevLevel < 0) {
			logger.error("CassandraAccessLevelUpdatar: unexpected access level, {}, for volume {}); access level not set in Cassandra", accessLevel, volumeId);
			return false;
		} else {
			// set the access level in Cassandra
			return this.accessLevelIngester.ingest(volumeId, abbrevLevel);
		}
	}
	
	// method "update" using RedisClient.getHashFieldValuesOnly instead of RedisClient.getHashFieldValues
	public List<String> update1(List<String> volumeIds) {
		// get the access levels of the volumes from redis
		List<String> volLevels = 
				redisClient.getHashFieldValuesOnly(volumeIds.stream().map(volumeId -> RedisRightsUtils.volumeIdToRedisKey(volumeId)).collect(Collectors.toList()), this.redisAccessLevelFieldName);
		// set the access levels in Cassandra
		return processVolInfo(volumeIds, volLevels, this::updateAccessLevel);
	}
	
	// given a list of volume ids and a list of items that represent information about the volumes, e.g., access levels, process 
	// volume ids and the associated volume information; note that volInfoProcessor may have side-effects
	// result: the list of volume ids for which processing failed
    private <T> List<String> processVolInfo(List<String> volumeIds, List<T> volInfo, BiFunction<String, T, Boolean> volInfoProcessor) {
    	Iterator<String> volIdsItr = volumeIds.iterator();
    	Iterator<T> volInfoItr = volInfo.iterator();
    	
    	List<String> result = new ArrayList<String>();
    	
    	while (volIdsItr.hasNext() && volInfoItr.hasNext()) {
    		String volId = volIdsItr.next();
    		if (!volInfoProcessor.apply(volId, volInfoItr.next())) {
    			result.add(volId);
    		}
    	}
    	return result;
    }
}
