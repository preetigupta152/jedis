package redis.clients.jedis.shardrebalancer;


import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import com.google.common.base.Stopwatch;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.shardedcluster.ShardedJedisCluster;

public class ShardRebalancer {

	
	private JedisShardInfo source = null;
	private static JedisPool sourcePool = null;
	
	protected static final String REDIS_MAX_POOL_SIZE = "redis.maxPoolSize";
	protected static final String REDIS_SOURCE_HOST = "redis.source.host";
	protected static final String REDIS_SOURCE_PORT = "redis.source.port";
	protected static final String REDIS_SOURCE_TIMEOUT = "redis.source.timeout";
	protected static final String JEDIS_CLUSTER_CONFIG = "jedis.cluster.config";
	
  
	
  public void intializeJedisPool() {
  	
  	final String METHOD_NAME = "intializeJedisPool";  
  	
  	String clusterConfigFile= System.getProperty(JEDIS_CLUSTER_CONFIG);
    ShardedJedisCluster.bootstrap(clusterConfigFile);
    
    String redisHost = System.getProperty(REDIS_SOURCE_HOST);
    String redisPort = System.getProperty(REDIS_SOURCE_PORT);
    String redisTimeOut = System.getProperty(REDIS_SOURCE_TIMEOUT);
    
    this.source = new JedisShardInfo(redisHost, redisPort);

    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
      
    poolConfig.setBlockWhenExhausted(GenericObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED);

    // Maximum active connections to Redis instance
    poolConfig.setMaxTotal(Integer.valueOf(System.getProperty(REDIS_MAX_POOL_SIZE)));
    //poolConfig.setMaxTotal(Integer.valueOf("10000")); //For testing
   
    // Number of connections to Redis that just sit there and do nothing
    poolConfig.setMaxIdle(10);
    // Minimum number of idle connections to Redis
    // These can be seen as always open and ready to serve
    poolConfig.setMinIdle(5);
    // Tests whether connections are dead during idle periods
    poolConfig.setTestWhileIdle(false);
    // Tests whether connection is dead when connection retrieval method is called
    poolConfig.setTestOnBorrow(false);
    // Tests whether connection is dead when returning a connection to the pool
    poolConfig.setTestOnReturn(false);
   
   sourcePool = new JedisPool(poolConfig, redisHost, Integer.valueOf(redisPort), Integer.valueOf(redisTimeOut));

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
	
	private void rebalance(){

		int threadPoolSize = 50;
		int queueSize = 100; // recommended - twice the size of the poolSize
		int threadKeepAliveTime = 5;
		TimeUnit threadKeepAliveTimeUnit = TimeUnit.SECONDS;
		int maxBlockingTime = 10;
		TimeUnit maxBlockingTimeUnit = TimeUnit.MILLISECONDS;
		Callable<Boolean> blockingTimeoutCallback = new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				System.out.println("*** Still waiting for task insertion... ***");
				return true; // keep waiting
			}
		};
		
		//-----------------------------------------------------------------
		// Create the NotifyingBlockingThreadPoolExecutor
		//-----------------------------------------------------------------
		NotifyingBlockingThreadPoolExecutor threadPoolExecutor =
			new NotifyingBlockingThreadPoolExecutor(threadPoolSize, queueSize,
											threadKeepAliveTime, threadKeepAliveTimeUnit,
											maxBlockingTime, maxBlockingTimeUnit, blockingTimeoutCallback);
	
		long start = System.currentTimeMillis();
		Timer timer = new Timer();
		Stopwatch stopwatch = Stopwatch.createStarted();
	  
		Jedis sourceJedis = null;
		
		try{
		sourceJedis = sourcePool.getResource();
		
		ScanParams param = new ScanParams();
		param.count(1000);
		
		String cursor = String.valueOf(0);
		ScanResult<String> result = null;
		do {
			result = sourceJedis.scan(cursor);
			cursor = result.getStringCursor();
			Migrator migrator = new Migrator(sourcePool, result);
			threadPoolExecutor.submit(migrator);
				
		}while(!cursor.equals(String.valueOf(0)));

		//-----------------------------------------------------------------
		// All tasks were sent...
		//-----------------------------------------------------------------
		System.out.println("Almost Done...");

		//-----------------------------------------------------------------
		// Wait for the last tasks to be done
		//-----------------------------------------------------------------
		try {
			boolean done = false;
			do {
				System.out.println("waiting for the last tasks to finish");				
				// we don't want to use here awaitTermination, which is relevant only if shutdown was called
				// in our case we don't call shutdown - we want to know that all tasks sent so far are finished
				//done = threadPoolExecutor.await(20, TimeUnit.MILLISECONDS);
				done = threadPoolExecutor.await(20, TimeUnit.MILLISECONDS);
			} while(!done);
			System.out.println("Outstanding tasks = " + threadPoolExecutor.getTaskCount());
			System.out.println("Completed tasks = " + threadPoolExecutor.getCompletedTaskCount());
		} catch (InterruptedException e) {
			System.out.println(e);
		}
		
		}finally{
			if(sourceJedis != null){
				sourcePool.returnResource(sourceJedis);
			}
		}
		 
		 stopwatch.stop(); // optional

	   long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);

	   System.out.println("**************Time taken to Rebalance the shards : " + stopwatch);
	   //threadPoolExecutor.shutdown();
	   System.out.println("**************After shutdown : ");
	}
	
	Boolean isSource(JedisShardInfo jsi){
		
		if(jsi.getHost().equalsIgnoreCase(source.getHost()) 
									&& jsi.getPort() == source.getPort()){
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
	
	public static void  main(String[] args){
		ShardRebalancer rebalancer = new ShardRebalancer();
		rebalancer.intializeJedisPool();
		rebalancer.rebalance();
	}
	
}
