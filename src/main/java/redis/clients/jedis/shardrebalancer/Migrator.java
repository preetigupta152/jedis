package redis.clients.jedis.shardrebalancer;

import java.util.concurrent.Callable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.shardedcluster.ShardedJedisCluster;

public class Migrator implements Callable<Integer> {

	JedisPool sourcePool;
	
	ScanResult<String> result = null;
	
	
	
	public Migrator(JedisPool sourcePool, ScanResult<String> result) {
	  super();
	  this.sourcePool = sourcePool;
	  this.result = result;
  }
	
	
	private void migrate() {
		Jedis sourceJedis = sourcePool.getResource();
		String sourceHost = sourceJedis.getClient().getHost();
		int sourcePort = sourceJedis.getClient().getPort();
		
		if(sourceJedis != null){
			sourcePool.returnResource(sourceJedis);
		}
		ShardedJedisPool sjPool = null;
		ShardedJedis sJedis = null;
		sjPool = ShardedJedisCluster.getPool();
		sJedis = sjPool.getResource();
		Multimap<String, String> keysToMigrate = HashMultimap.create();
		try {

			// Check each key and add to keysToMigrate
			for (String key : result.getResult()) {
				Jedis j = sJedis.getShard(key);
				if (!j.getClient().getHost().equalsIgnoreCase(sourceHost)) {
					keysToMigrate.put(sJedis.getKeyTag(key), key);
				}else if(j.getClient().getPort() != (sourcePort)) {
					keysToMigrate.put(sJedis.getKeyTag(key), key);
				}

			}

			int count = 0;
			for (String hashTag : keysToMigrate.keySet()) {
				Jedis j = sJedis.getShard(hashTag);
				String host = j.getClient().getHost();
				int port = j.getClient().getPort();
				Jedis pJedis = null;
				Pipeline pipeline = null;
				Boolean ex = false;
				try {
					pJedis = sourcePool.getResource();
				
					pipeline = pJedis.pipelined();

					for (String key : keysToMigrate.get(hashTag)) {
						pipeline.migrate(host,port, key, 0, 5000);

					}

					pipeline.sync();
					
				}catch (Exception e){
					//pipeline.sync();
					System.out.println("Exception in Migrator************" );
					 e.printStackTrace();
					 ex = true;
					if (pJedis != null) {
						sourcePool.returnBrokenResource(pJedis);
					}
				} finally {

					if (pJedis != null && !ex) {
						sourcePool.returnResource(pJedis);
					}
				}
				count++;

			}
			System.out.println("Number of HashTag - " + count);
		} finally {
			 if (sJedis != null) {
				sjPool.returnResource(sJedis);
			} 
		}
	}

	public Integer call() {
		try {
			migrate();
			
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return 0;
	}
}
