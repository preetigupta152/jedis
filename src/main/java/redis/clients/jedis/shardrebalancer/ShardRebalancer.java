package redis.clients.jedis.shardrebalancer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.google.common.base.Stopwatch;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.shardedcluster.ShardedJedisCluster;

public class ShardRebalancer {

	private JedisShardInfo source = null;
	private static JedisPool sourcePool = null;

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

	private static final String REDIS_SOURCE_HOST = "jedis.source.host";

	private static final String REDIS_SOURCE_PORT = "jedis.source.port";

	public void intializeJedisPool(String clusterConfigFile,
			Properties properties) {

		ShardedJedisCluster.bootstrap(clusterConfigFile, properties);

		int timeout = properties != null ? Integer.valueOf(properties
				.getProperty(REDIS_TIMEOUT, "2000")) : 2000;
		System.out.println("timeout " + timeout);

		String redisHost = properties != null ? properties
				.getProperty(REDIS_SOURCE_HOST) : null;
		Integer redisPort = (properties != null && properties
				.getProperty(REDIS_SOURCE_PORT) != null) ? Integer
				.valueOf(properties.getProperty(REDIS_SOURCE_PORT)) : null;

		if (redisHost == null || redisPort == null)
			throw new JedisException(
					"One or both System properties jedis.source.host , jedis.source.port are not specified.");

		this.source = new JedisShardInfo(redisHost, redisPort, timeout);

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

		if (properties != null) {
			poolConfig.setMaxTotal(Integer.valueOf(properties.getProperty(
					MAX_ACTIVE, "100")));
			poolConfig.setMaxIdle(Integer.valueOf(properties.getProperty(
					MAX_IDLE, "10")));
			poolConfig.setMinIdle(Integer.valueOf(properties.getProperty(
					MIN_IDLE, "5")));
			poolConfig
					.setBlockWhenExhausted(Boolean.valueOf(properties.getProperty(
							BLOCK_WHEN_EXHAUSTED,
							String.valueOf(GenericObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED))));

			String maxWaitMilli = properties.getProperty(MAX_WAIT);
			if (maxWaitMilli != null)
				poolConfig.setMaxWaitMillis(Integer.valueOf(properties
						.getProperty(MAX_WAIT)));

			String minEvictableIdleTimeMillis = properties
					.getProperty(MIN_EVICTABLE_IDLE_TIME_MS);
			if (minEvictableIdleTimeMillis != null)
				poolConfig.setMinEvictableIdleTimeMillis(Integer
						.valueOf(properties
								.getProperty(MIN_EVICTABLE_IDLE_TIME_MS)));

			String timeBetweenEvictionRunsMillis = properties
					.getProperty(TIME_BETWEEN_EVICTION_MS);
			if (timeBetweenEvictionRunsMillis != null)
				poolConfig.setTimeBetweenEvictionRunsMillis(Integer
						.valueOf(properties
								.getProperty(TIME_BETWEEN_EVICTION_MS)));

			String numTestsPerEvictionRun = properties
					.getProperty(NUMBER_OF_TESTS_EVICTION_RUN);
			if (numTestsPerEvictionRun != null)
				poolConfig.setNumTestsPerEvictionRun(Integer.valueOf(properties
						.getProperty(NUMBER_OF_TESTS_EVICTION_RUN)));

			poolConfig.setTestWhileIdle(Boolean.valueOf(properties.getProperty(
					TEST_WHILE_IDLE, "false")));
			poolConfig.setTestOnBorrow(Boolean.valueOf(properties.getProperty(
					TEST_ON_BORROW, "false")));
			poolConfig.setTestOnReturn(Boolean.valueOf(properties.getProperty(
					TEST_ON_RETURN, "false")));
		} else {
			poolConfig.setMaxTotal(100);
			poolConfig.setMaxIdle(10);
			poolConfig.setMinIdle(5);
			poolConfig
					.setBlockWhenExhausted(GenericObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED);
			poolConfig.setTestWhileIdle(Boolean.FALSE);
			poolConfig.setTestOnBorrow(Boolean.FALSE);
			poolConfig.setTestOnReturn(Boolean.FALSE);
		}

		sourcePool = new JedisPool(poolConfig, redisHost,
				Integer.valueOf(redisPort), timeout);

	}

	public ShardRebalancer() {
		super();
	}

	public ShardRebalancer(JedisShardInfo source) {
		super();
		this.source = source;
	}

	public ShardRebalancer(String host, String port) {
		super();
		this.source = new JedisShardInfo(host, port);
	}

	public void initiateRebalance(String clusterConfigFile,
			Properties properties) {
		intializeJedisPool(clusterConfigFile, properties);
		rebalance();
	}

	private void rebalance() {
		int migratorLaunchedCounter = 0;
		int migratorSucceededCounter = 0;
		int migratorFailedCounter = 0;
		List<Future<Boolean>> migratorList = new LinkedList<Future<Boolean>>();
		List<Boolean> migratorResultList = new LinkedList<Boolean>();
		ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(50);
		Stopwatch stopwatch = Stopwatch.createStarted();
		Jedis sourceJedis = null;

		try {
			sourceJedis = sourcePool.getResource();

			ScanParams param = new ScanParams();
			param.count(1000);

			String cursor = String.valueOf(0);
			ScanResult<String> result = null;
			do {
				result = sourceJedis.scan(cursor);
				cursor = result.getStringCursor();
				Migrator migrator = new Migrator(sourcePool, result);
				++migratorLaunchedCounter;
				migratorList.add(threadPoolExecutor.submit(migrator));
			} while (!cursor.equals(String.valueOf(0)));

			// -----------------------------------------------------------------
			// All tasks were sent...
			// -----------------------------------------------------------------
			System.out.println("Almost Done...");
			
			// -----------------------------------------------------------------
			// Wait for the last tasks to be done
			// -----------------------------------------------------------------
			System.out.println("waiting for the last tasks to finish");
			
			for (Future<Boolean> migrator : migratorList) {
				try {
					migratorResultList.add(migrator.get());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			
			for (Boolean migratorResult : migratorResultList) {
				if (migratorResult) {
					++migratorSucceededCounter;
				} else {
					++migratorFailedCounter;
				}
			}
			
			System.out.println("Tasks launched: " + migratorLaunchedCounter);
			System.out.println("Tasks succeeded: " + migratorSucceededCounter);
			System.out.println("Tasks failed: " + migratorFailedCounter);
		} finally {
			if (sourceJedis != null) {
				sourcePool.returnResource(sourceJedis);
			}
		}

		stopwatch.stop(); // optional

		System.out
				.println("**************Time taken to Rebalance the shards : "
						+ stopwatch);
		threadPoolExecutor.shutdown();
	}

	Boolean isSource(JedisShardInfo jsi) {
		if (jsi.getHost().equalsIgnoreCase(source.getHost())
				&& jsi.getPort() == source.getPort()) {
			return true;
		}

		return false;
	}

	public ShardedJedis getShardedResource() {
		ShardedJedis jedis;
		jedis = ShardedJedisCluster.getPool().getResource();
		return jedis;
	}

	public void returnShardedResource(final ShardedJedis jedis) {
		ShardedJedisCluster.getPool().returnResource(jedis);
	}

}
