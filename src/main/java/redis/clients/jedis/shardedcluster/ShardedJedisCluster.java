package redis.clients.jedis.shardedcluster;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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
	
	public static void bootstrap(String fileName){
		JedisShardConfigReader cfgReader = new JedisShardConfigReader();
		ClusterConfig cfg = cfgReader.loadConfig(new File(fileName));
		
		// Create JedisShardInfo and add to shards
		for(Node node : cfg.getNodes()){
			shards.add(new JedisShardInfo(node.getHost(), node.getPort(), node.getName()));
		}
		pool =  new ShardedJedisPool(new GenericObjectPoolConfig(), shards, ShardedJedis.DEFAULT_KEY_TAG_PATTERN);
	}
	
	public static ShardedJedisPool getPool(){
		return pool;
	}
	
	
}
