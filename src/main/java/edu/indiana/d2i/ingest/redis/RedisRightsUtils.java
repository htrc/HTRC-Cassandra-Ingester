package edu.indiana.d2i.ingest.redis;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.Configuration;

/*
 * Class that contains utility functions pertaining to HTRC rights information stored in Redis, e.g., conversion of HTRC volume ids to keys in Redis.
 */
public class RedisRightsUtils {
	private static String redisVolIdKeyPrefix = null;
	private static String redisVolIdKeySuffix = null;
	
	public static String volumeIdToRedisKey(String volumeId) {
		if (redisVolIdKeyPrefix == null) {
			redisVolIdKeyPrefix = Configuration.getProperty(Constants.PK_REDIS_VOLUME_ID_KEY_PREFIX, Constants.DEFAULT_REDIS_VOLUME_ID_KEY_PREFIX);
		}
		if (redisVolIdKeySuffix == null) {
			redisVolIdKeySuffix = Configuration.getProperty(Constants.PK_REDIS_VOLUME_ID_KEY_SUFFIX, Constants.DEFAULT_REDIS_VOLUME_ID_KEY_SUFFIX);
		}
		return (redisVolIdKeyPrefix + volumeId + redisVolIdKeySuffix);
	}
	
	// convert the access level string to an abbreviated form of type integer, e.g., "1A" to 1
	public static Integer abbrevAccessLevel(String accessLevel) {
		if (accessLevel == null) {
			return -1;
		} else {
			int i = 0;
			boolean loopCondn = true;
			StringBuffer sb = new StringBuffer();
			// obtain the integer prefix of accesLevel
			while ((i < accessLevel.length()) && loopCondn) {
				char c = accessLevel.charAt(i);
				if (Character.isDigit(c)) {
					sb.append(c);
					i++;
				} else {
					loopCondn = false;
				}
			}
			String res = sb.toString();
			return (res.equals("") ? -1 : Integer.parseInt(res));
		}
	}
}
