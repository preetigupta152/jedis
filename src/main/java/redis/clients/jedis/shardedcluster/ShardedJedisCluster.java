package redis.clients.jedis.shardedcluster;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * ShardedJedisCluster creates Redis client side sharded cluster
 * @author ngovindasamy
 *
 */
public class ShardedJedisCluster {

	private static List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
	private static ShardedJedisPool pool = null;
	
   private static final String REDIS_TIMEOUT = "redisTimeOut";

   private static final String MAX_ACTIVE = "jedisPoolConfig.maxActive";

   private static final String MAX_IDLE = "jedisPoolConfig.maxIdle";

   private static final String MIN_IDLE = "jedisPoolConfig.minIdle";

   private static final String TEST_WHILE_IDLE = "jedisPoolConfig.testWhileIdle";

   private static final String TEST_ON_BORROW = "jedisPoolConfig.testOnBorrow";

   private static final String TEST_ON_RETURN = "jedisPoolConfig.testOnReturn";

   private static final String MAX_WAIT = "jedisPoolConfig.maxWait";
   
   private static final String BLOCK_WHEN_EXHAUSTED = "jedisPoolConfig.blockWhenExhausted";   

   private static final String MIN_EVICTABLE_IDLE_TIME_MS = "jedisPoolConfig.minEvictableIdleTimeMillis";

   private static final String TIME_BETWEEN_EVICTION_MS = "jedisPoolConfig.timeBetweenEvictionRunsMillis";

   private static final String NUMBER_OF_TESTS_EVICTION_RUN = "jedisPoolConfig.numTestsPerEvictionRun";	
   
   
	public static void bootstrap(String fileName , Properties properties){
		JedisShardConfigReader cfgReader = new JedisShardConfigReader();
		ClusterConfig cfg = cfgReader.loadConfig(new File(fileName));
		
		int timeout = properties != null ? Integer.valueOf(properties.getProperty(REDIS_TIMEOUT, "2000")) : 2000;
		System.out.println("timeout " + timeout);
		
		// Create JedisShardInfo and add to shards
		for(Node node : cfg.getNodes()){
			shards.add(new JedisShardInfo(node.getHost(), node.getPort(), node.getName(), cfg.getWeight(), timeout));
		}
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		
	      // Maximum active connections to Redis instance
		if (properties != null){
			poolConfig.setMaxTotal(Integer.valueOf(properties.getProperty(MAX_ACTIVE, "100")));
			poolConfig.setMaxIdle(Integer.valueOf(properties.getProperty(MAX_IDLE, "10")));
			poolConfig.setMinIdle(Integer.valueOf(properties.getProperty(MIN_IDLE, "5")));
			poolConfig.setBlockWhenExhausted(Boolean.valueOf(properties.getProperty(BLOCK_WHEN_EXHAUSTED, String.valueOf(GenericObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED))));
			
			String maxWaitMilli = properties.getProperty(MAX_WAIT);
			if(maxWaitMilli != null)
				poolConfig.setMaxWaitMillis(Integer.valueOf(properties.getProperty(MAX_WAIT)));
			
			String minEvictableIdleTimeMillis = properties.getProperty(MIN_EVICTABLE_IDLE_TIME_MS);
			if(minEvictableIdleTimeMillis != null)
				poolConfig.setMinEvictableIdleTimeMillis(Integer.valueOf(properties.getProperty(MIN_EVICTABLE_IDLE_TIME_MS)));
			
			String timeBetweenEvictionRunsMillis = properties.getProperty(TIME_BETWEEN_EVICTION_MS);
			if(timeBetweenEvictionRunsMillis != null)
				poolConfig.setTimeBetweenEvictionRunsMillis(Integer.valueOf(properties.getProperty(TIME_BETWEEN_EVICTION_MS)));
			
			String numTestsPerEvictionRun = properties.getProperty(NUMBER_OF_TESTS_EVICTION_RUN);
			if(numTestsPerEvictionRun != null)
				poolConfig.setNumTestsPerEvictionRun(Integer.valueOf(properties.getProperty(NUMBER_OF_TESTS_EVICTION_RUN)));
		    
			poolConfig.setTestWhileIdle(Boolean.valueOf(properties.getProperty(TEST_WHILE_IDLE, "false")));
		    poolConfig.setTestOnBorrow(Boolean.valueOf(properties.getProperty(TEST_ON_BORROW, "false")));
		    poolConfig.setTestOnReturn(Boolean.valueOf(properties.getProperty(TEST_ON_RETURN, "false")));
		}else{
			poolConfig.setMaxTotal(100);
			poolConfig.setMaxIdle(10);
			poolConfig.setMinIdle(5);
			poolConfig.setBlockWhenExhausted(GenericObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED);
		    poolConfig.setTestWhileIdle(Boolean.FALSE);
		    poolConfig.setTestOnBorrow(Boolean.FALSE);
		    poolConfig.setTestOnReturn(Boolean.FALSE);
		}
	    
		pool =  new ShardedJedisPool(poolConfig, shards, ShardedJedis.DEFAULT_KEY_TAG_PATTERN);
	}
	
	public static ShardedJedisPool getPool(){
		return pool;
	}
	
	
}
