package edu.indiana.d2i.ingest.redis;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/*
 * Class that interacts with Redis, i.e., gets values from and sets values in Redis. 
 */
public class RedisClient {
	private static Logger logger = LogManager.getLogger(RedisClient.class);

	private JedisPool jedisPool;
	private int numHgetsPerPipeline;
	private int numHsetsPerPipeline;
	
	public RedisClient() {
		String redisHost = Configuration.getProperty(Constants.PK_REDIS_HOST, Constants.DEFAULT_REDIS_HOST);
		this.jedisPool = new JedisPool(new JedisPoolConfig(), redisHost);
		this.numHgetsPerPipeline = Integer.parseInt(Configuration.getProperty(Constants.PK_REDIS_NUM_HGETS_PER_PIPELINE, Constants.DEFAULT_REDIS_NUM_HGETS_PER_PIPELINE));
		this.numHsetsPerPipeline = Integer.parseInt(Configuration.getProperty(Constants.PK_REDIS_NUM_HSETS_PER_PIPELINE, Constants.DEFAULT_REDIS_NUM_HSETS_PER_PIPELINE));
	}
	
	// returns the value of the specified field in the hash at the given key in Redis
	public String getHashFieldValue(String key, String fieldName) {
        try (Jedis jedis = this.jedisPool.getResource()) {
        	String value = jedis.hget(key, fieldName);
        	return value;
        } catch (Exception e) {
        	logger.error("Exception while trying to access redis: {}", e.getMessage(), e); 
        	return "";
        }
	}

	// sets the value of the specified field in the hash at the given key in Redis
	public boolean setHashFieldValue(String key, String fieldName, String fieldValue) {
        try (Jedis jedis = this.jedisPool.getResource()) {
        	long hsetResult = jedis.hset(key, fieldName, fieldValue);
        	return true;
        } catch (Exception e) {
        	logger.error("Exception while trying to access redis: {}", e.getMessage(), e); 
        	return false;
        }
	}
	
	// given a list of elements (e.g., volume id v), the name of a hash field in Redis, and a function to construct keys in Redis from the
	// aforementioned elements (e.g., the Redis key for v may be volume:v:info), constructs the keys corresponding to the elements, and obtains 
	// the values of the field in the hashes at the keys in Redis
	// result: a list of (keyElem, hashFieldValue) pairs
	public <T> List<Map.Entry<T, String>> getHashFieldValues(List<T> keyElems, String fieldName, Function<T, String> redisKeyCreator) {
		if ((keyElems == null) || (keyElems.size() == 0)) {
			return Collections.emptyList();
		}
		
        try (Jedis jedis = this.jedisPool.getResource()) {
        	Pipeline pipeline = jedis.pipelined();
        	int size = keyElems.size();
        	int i = 0;
        	List<Map.Entry<T, String>> res = new ArrayList<Map.Entry<T, String>>();
        	while (i < size) {
        		int numHgets = 0;
        		List<Map.Entry<T, Response<String>>> batchRes = new ArrayList<Map.Entry<T, Response<String>>>(numHgetsPerPipeline);
        		while ((i < size) && (numHgets < numHgetsPerPipeline)) {
        			T keyElem = keyElems.get(i);
        			batchRes.add(new AbstractMap.SimpleEntry<>(keyElem, pipeline.hget(redisKeyCreator.apply(keyElem), fieldName)));
        			i++;
        			numHgets++;
        		}
        		pipeline.sync();
        		batchRes.forEach(entry -> res.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().get())));
        	}
        	return res;
        } catch (Exception e) {
        	logger.error("Exception while trying to access redis: {}", e.getMessage(), e); 
        	return Collections.emptyList();
        }
	}
	
	// returns a list of values of the specified field in the hashes at the given keys in Redis; this method differs from getHashFieldValues in that
	// it returns only the values, and not mappings of key elements to values like the latter
	public List<String> getHashFieldValuesOnly(List<String> keys, String fieldName) {		
		if ((keys == null) || (keys.size() == 0)) {
			return Collections.emptyList();
		}
		
        try (Jedis jedis = this.jedisPool.getResource()) {
        	Pipeline pipeline = jedis.pipelined();
        	int size = keys.size();
        	int i = 0;
        	List<String> result = new ArrayList<String>();
        	while (i < size) {
        		int numHgets = 0;
        		List<Response<String>> batchRes = new ArrayList<Response<String>>(numHgetsPerPipeline);
        		while ((i < size) && (numHgets < numHgetsPerPipeline)) {
        			String key = keys.get(i);
        			batchRes.add(pipeline.hget(key, fieldName));
        			i++;
        			numHgets++;
        		}
        		pipeline.sync();
        		// System.out.println(batchRes.stream().map(response -> response.get()).collect(Collectors.joining(",", "[", "]")));
        		batchRes.forEach(response -> result.add(response.get()));
        	}
        	return result;
        } catch (Exception e) {
        	logger.error("Exception while trying to access redis: {}", e.getMessage(), e); 
        	return Collections.emptyList();
        }
	}
	
	// convenience function to assign the same value, fieldValue, to a specified hash field at all given keys in Redis
	public boolean setHashFieldValues(List<String> keys, String fieldName, String fieldValue) {
		return setHashFieldValues(keys.stream().map(key -> new AbstractMap.SimpleEntry<>(key, fieldValue)).collect(Collectors.toList()), fieldName);
	}

	// sets the values of a specified field of hashes at given keys in Redis; keyValueList contains a mapping of Redis keys to the values to 
	// be assigned to the hash field at those keys; the fieldName provides the name of the hash field to be set
	// result: true if all assignments were successful, false otherwise
	public boolean setHashFieldValues(List<Map.Entry<String, String>> keyValueList, String fieldName) {
		int size = keyValueList.size();
		if ((keyValueList == null) || (size == 0)) {
			return true;
		}
		
        try (Jedis jedis = this.jedisPool.getResource()) {
        	Pipeline pipeline = jedis.pipelined();
        	int i = 0;
        	while (i < size) {
        		int numHsets = 0;
        		while ((i < size) && (numHsets < numHsetsPerPipeline)) {
        			Map.Entry<String, String> entry = keyValueList.get(i);
        			pipeline.hset(entry.getKey(), fieldName, entry.getValue());
        			numHsets++;
        			i++;
        		}
        		pipeline.sync();
        	}
        	return true;
        } catch (Exception e) {
        	logger.error("Exception while trying to access redis: {}", e.getMessage(), e); 
        	return false;
        }
	}
}
