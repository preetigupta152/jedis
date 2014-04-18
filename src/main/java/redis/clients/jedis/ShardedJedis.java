package redis.clients.jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.Hashing;

public class ShardedJedis extends BinaryShardedJedis implements JedisCommands, 
											MultiKeyCommands{
    public ShardedJedis(List<JedisShardInfo> shards) {
	super(shards);
    }

    public ShardedJedis(List<JedisShardInfo> shards, Hashing algo) {
	super(shards, algo);
    }

    public ShardedJedis(List<JedisShardInfo> shards, Pattern keyTagPattern) {
	super(shards, keyTagPattern);
    }

    public ShardedJedis(List<JedisShardInfo> shards, Hashing algo,
	    Pattern keyTagPattern) {
	super(shards, algo, keyTagPattern);
    }

    public String set(String key, String value) {
	Jedis j = getShard(key);
	return j.set(key, value);
    }

    public String get(String key) {
	Jedis j = getShard(key);
	return j.get(key);
    }

    public String echo(String string) {
	Jedis j = getShard(string);
	return j.echo(string);
    }

    public Boolean exists(String key) {
	Jedis j = getShard(key);
	return j.exists(key);
    }

    public String type(String key) {
	Jedis j = getShard(key);
	return j.type(key);
    }

    public Long expire(String key, int seconds) {
	Jedis j = getShard(key);
	return j.expire(key, seconds);
    }

    public Long expireAt(String key, long unixTime) {
	Jedis j = getShard(key);
	return j.expireAt(key, unixTime);
    }

    public Long ttl(String key) {
	Jedis j = getShard(key);
	return j.ttl(key);
    }

    public Boolean setbit(String key, long offset, boolean value) {
	Jedis j = getShard(key);
	return j.setbit(key, offset, value);
    }

    public Boolean setbit(String key, long offset, String value) {
	Jedis j = getShard(key);
	return j.setbit(key, offset, value);
    }

    public Boolean getbit(String key, long offset) {
	Jedis j = getShard(key);
	return j.getbit(key, offset);
    }

    public Long setrange(String key, long offset, String value) {
	Jedis j = getShard(key);
	return j.setrange(key, offset, value);
    }

    public String getrange(String key, long startOffset, long endOffset) {
	Jedis j = getShard(key);
	return j.getrange(key, startOffset, endOffset);
    }

    public String getSet(String key, String value) {
	Jedis j = getShard(key);
	return j.getSet(key, value);
    }

    public Long setnx(String key, String value) {
	Jedis j = getShard(key);
	return j.setnx(key, value);
    }

    public String setex(String key, int seconds, String value) {
	Jedis j = getShard(key);
	return j.setex(key, seconds, value);
    }

    public List<String> blpop(String arg) {
	Jedis j = getShard(arg);
	return j.blpop(arg);
    }

    public List<String> brpop(String arg) {
	Jedis j = getShard(arg);
	return j.brpop(arg);
    }

    public Long decrBy(String key, long integer) {
	Jedis j = getShard(key);
	return j.decrBy(key, integer);
    }

    public Long decr(String key) {
	Jedis j = getShard(key);
	return j.decr(key);
    }

    public Long incrBy(String key, long integer) {
	Jedis j = getShard(key);
	return j.incrBy(key, integer);
    }

    public Long incr(String key) {
	Jedis j = getShard(key);
	return j.incr(key);
    }

    public Long append(String key, String value) {
	Jedis j = getShard(key);
	return j.append(key, value);
    }

    public String substr(String key, int start, int end) {
	Jedis j = getShard(key);
	return j.substr(key, start, end);
    }

    public Long hset(String key, String field, String value) {
	Jedis j = getShard(key);
	return j.hset(key, field, value);
    }

    public String hget(String key, String field) {
	Jedis j = getShard(key);
	return j.hget(key, field);
    }

    public Long hsetnx(String key, String field, String value) {
	Jedis j = getShard(key);
	return j.hsetnx(key, field, value);
    }

    public String hmset(String key, Map<String, String> hash) {
	Jedis j = getShard(key);
	return j.hmset(key, hash);
    }

    public List<String> hmget(String key, String... fields) {
	Jedis j = getShard(key);
	return j.hmget(key, fields);
    }

    public Long hincrBy(String key, String field, long value) {
	Jedis j = getShard(key);
	return j.hincrBy(key, field, value);
    }

    public Boolean hexists(String key, String field) {
	Jedis j = getShard(key);
	return j.hexists(key, field);
    }

    public Long del(String key) {
	Jedis j = getShard(key);
	return j.del(key);
    }

    public Long hdel(String key, String... fields) {
	Jedis j = getShard(key);
	return j.hdel(key, fields);
    }

    public Long hlen(String key) {
	Jedis j = getShard(key);
	return j.hlen(key);
    }

    public Set<String> hkeys(String key) {
	Jedis j = getShard(key);
	return j.hkeys(key);
    }

    public List<String> hvals(String key) {
	Jedis j = getShard(key);
	return j.hvals(key);
    }

    public Map<String, String> hgetAll(String key) {
	Jedis j = getShard(key);
	return j.hgetAll(key);
    }

    public Long rpush(String key, String... strings) {
	Jedis j = getShard(key);
	return j.rpush(key, strings);
    }

    public Long lpush(String key, String... strings) {
	Jedis j = getShard(key);
	return j.lpush(key, strings);
    }

    public Long lpushx(String key, String... string) {
	Jedis j = getShard(key);
	return j.lpushx(key, string);
    }

    public Long strlen(final String key) {
	Jedis j = getShard(key);
	return j.strlen(key);
    }

    public Long move(String key, int dbIndex) {
	Jedis j = getShard(key);
	return j.move(key, dbIndex);
    }

    public Long rpushx(String key, String... string) {
	Jedis j = getShard(key);
	return j.rpushx(key, string);
    }

    public Long persist(final String key) {
	Jedis j = getShard(key);
	return j.persist(key);
    }

    public Long llen(String key) {
	Jedis j = getShard(key);
	return j.llen(key);
    }

    public List<String> lrange(String key, long start, long end) {
	Jedis j = getShard(key);
	return j.lrange(key, start, end);
    }

    public String ltrim(String key, long start, long end) {
	Jedis j = getShard(key);
	return j.ltrim(key, start, end);
    }

    public String lindex(String key, long index) {
	Jedis j = getShard(key);
	return j.lindex(key, index);
    }

    public String lset(String key, long index, String value) {
	Jedis j = getShard(key);
	return j.lset(key, index, value);
    }

    public Long lrem(String key, long count, String value) {
	Jedis j = getShard(key);
	return j.lrem(key, count, value);
    }

    public String lpop(String key) {
	Jedis j = getShard(key);
	return j.lpop(key);
    }

    public String rpop(String key) {
	Jedis j = getShard(key);
	return j.rpop(key);
    }

    public Long sadd(String key, String... members) {
	Jedis j = getShard(key);
	return j.sadd(key, members);
    }

    public Set<String> smembers(String key) {
	Jedis j = getShard(key);
	return j.smembers(key);
    }

    public Long srem(String key, String... members) {
	Jedis j = getShard(key);
	return j.srem(key, members);
    }

    public String spop(String key) {
	Jedis j = getShard(key);
	return j.spop(key);
    }

    public Long scard(String key) {
	Jedis j = getShard(key);
	return j.scard(key);
    }

    public Boolean sismember(String key, String member) {
	Jedis j = getShard(key);
	return j.sismember(key, member);
    }

    public String srandmember(String key) {
	Jedis j = getShard(key);
	return j.srandmember(key);
    }

    public Long zadd(String key, double score, String member) {
	Jedis j = getShard(key);
	return j.zadd(key, score, member);
    }

    public Long zadd(String key, Map<String, Double> scoreMembers) {
	Jedis j = getShard(key);
	return j.zadd(key, scoreMembers);
    }

    public Set<String> zrange(String key, long start, long end) {
	Jedis j = getShard(key);
	return j.zrange(key, start, end);
    }

    public Long zrem(String key, String... members) {
	Jedis j = getShard(key);
	return j.zrem(key, members);
    }

    public Double zincrby(String key, double score, String member) {
	Jedis j = getShard(key);
	return j.zincrby(key, score, member);
    }

    public Long zrank(String key, String member) {
	Jedis j = getShard(key);
	return j.zrank(key, member);
    }

    public Long zrevrank(String key, String member) {
	Jedis j = getShard(key);
	return j.zrevrank(key, member);
    }

    public Set<String> zrevrange(String key, long start, long end) {
	Jedis j = getShard(key);
	return j.zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
	Jedis j = getShard(key);
	return j.zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
	Jedis j = getShard(key);
	return j.zrevrangeWithScores(key, start, end);
    }

    public Long zcard(String key) {
	Jedis j = getShard(key);
	return j.zcard(key);
    }

    public Double zscore(String key, String member) {
	Jedis j = getShard(key);
	return j.zscore(key, member);
    }

    public List<String> sort(String key) {
	Jedis j = getShard(key);
	return j.sort(key);
    }

    public List<String> sort(String key, SortingParams sortingParameters) {
	Jedis j = getShard(key);
	return j.sort(key, sortingParameters);
    }

    public Long zcount(String key, double min, double max) {
	Jedis j = getShard(key);
	return j.zcount(key, min, max);
    }

    public Long zcount(String key, String min, String max) {
	Jedis j = getShard(key);
	return j.zcount(key, min, max);
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
	Jedis j = getShard(key);
	return j.zrangeByScore(key, min, max);
    }

    public Set<String> zrevrangeByScore(String key, double max, double min) {
	Jedis j = getShard(key);
	return j.zrevrangeByScore(key, max, min);
    }

    public Set<String> zrangeByScore(String key, double min, double max,
	    int offset, int count) {
	Jedis j = getShard(key);
	return j.zrangeByScore(key, min, max, offset, count);
    }

    public Set<String> zrevrangeByScore(String key, double max, double min,
	    int offset, int count) {
	Jedis j = getShard(key);
	return j.zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
	Jedis j = getShard(key);
	return j.zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
	    double min) {
	Jedis j = getShard(key);
	return j.zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min,
	    double max, int offset, int count) {
	Jedis j = getShard(key);
	return j.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
	    double min, int offset, int count) {
	Jedis j = getShard(key);
	return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Set<String> zrangeByScore(String key, String min, String max) {
	Jedis j = getShard(key);
	return j.zrangeByScore(key, min, max);
    }

    public Set<String> zrevrangeByScore(String key, String max, String min) {
	Jedis j = getShard(key);
	return j.zrevrangeByScore(key, max, min);
    }

    public Set<String> zrangeByScore(String key, String min, String max,
	    int offset, int count) {
	Jedis j = getShard(key);
	return j.zrangeByScore(key, min, max, offset, count);
    }

    public Set<String> zrevrangeByScore(String key, String max, String min,
	    int offset, int count) {
	Jedis j = getShard(key);
	return j.zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
	Jedis j = getShard(key);
	return j.zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max,
	    String min) {
	Jedis j = getShard(key);
	return j.zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min,
	    String max, int offset, int count) {
	Jedis j = getShard(key);
	return j.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max,
	    String min, int offset, int count) {
	Jedis j = getShard(key);
	return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Long zremrangeByRank(String key, long start, long end) {
	Jedis j = getShard(key);
	return j.zremrangeByRank(key, start, end);
    }

    public Long zremrangeByScore(String key, double start, double end) {
	Jedis j = getShard(key);
	return j.zremrangeByScore(key, start, end);
    }

    public Long zremrangeByScore(String key, String start, String end) {
	Jedis j = getShard(key);
	return j.zremrangeByScore(key, start, end);
    }

    public Long linsert(String key, LIST_POSITION where, String pivot,
	    String value) {
	Jedis j = getShard(key);
	return j.linsert(key, where, pivot, value);
    }

    public Long bitcount(final String key) {
	Jedis j = getShard(key);
	return j.bitcount(key);
    }

    public Long bitcount(final String key, long start, long end) {
	Jedis j = getShard(key);
	return j.bitcount(key, start, end);
    }

    @Deprecated
    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long)
     * And will be removed on next major release
     * @see https://github.com/xetorthio/jedis/issues/531 
     */
    public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
	Jedis j = getShard(key);
	return j.hscan(key, cursor);
    }

    @Deprecated
    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long)
     * And will be removed on next major release
     * @see https://github.com/xetorthio/jedis/issues/531 
     */
    public ScanResult<String> sscan(String key, int cursor) {
	Jedis j = getShard(key);
	return j.sscan(key, cursor);
    }

    @Deprecated
    /**
     * This method is deprecated due to bug (scan cursor should be unsigned long)
     * And will be removed on next major release
     * @see https://github.com/xetorthio/jedis/issues/531 
     */
    public ScanResult<Tuple> zscan(String key, int cursor) {
	Jedis j = getShard(key);
	return j.zscan(key, cursor);
    }
    
    public ScanResult<Entry<String, String>> hscan(String key, final String cursor) {
	Jedis j = getShard(key);
	return j.hscan(key, cursor);
    }
    
    public ScanResult<String> sscan(String key, final String cursor) {
	Jedis j = getShard(key);
	return j.sscan(key, cursor);
    }
    
    public ScanResult<Tuple> zscan(String key, final String cursor) {
	Jedis j = getShard(key);
	return j.zscan(key, cursor);
    }

    
    //TODO Stubs for MultiKeyCommands interface methods
	@Override
	public Long del(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> blpop(int timeout, String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> brpop(int timeout, String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> blpop(String... args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> brpop(String... args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> keys(String pattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> mget(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String mset(String... keysvalues) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long msetnx(String... keysvalues) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String rename(String oldkey, String newkey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long renamenx(String oldkey, String newkey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String rpoplpush(String srckey, String dstkey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> sdiff(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long sdiffstore(String dstkey, String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

    //TODO Implement multikeyjediscommands interface
    /** This is copies from Jedis
     * Return the members of a set resulting from the intersection of all the
     * sets hold at the specified keys. Like in
     * {@link #lrange(String, long, long) LRANGE} the result is sent to the
     * client as a multi-bulk reply (see the protocol specification for more
     * information). If just a single key is specified, then this command
     * produces the same result as {@link #smembers(String) SMEMBERS}. Actually
     * SMEMBERS is just syntax sugar for SINTER.
     * <p>
     * Non existing keys are considered like empty sets, so if one of the keys
     * is missing an empty set is returned (since the intersection with an empty
     * set always is an empty set).
     * <p>
     * Time complexity O(N*M) worst case where N is the cardinality of the
     * smallest set and M the number of sets
     * 
     * @param keys
     * @return Multi bulk reply, specifically the list of common elements.
     */
	@Override
	public Set<String> sinter(final String... keys) {
		checkIsInMulti(keys[0]);
		Jedis j = getShard(keys[0]);
		return j.sinter(keys);
	}
	



	@Override
	public Long sinterstore(String dstkey, String... keys) {
		checkIsInMulti(keys[0]);
		Jedis j = getShard(keys[0]);
		return j.sinterstore(dstkey, keys);
	}

	@Override
	public Long smove(String srckey, String dstkey, String member) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long sort(String key, SortingParams sortingParameters, String dstkey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long sort(String key, String dstkey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> sunion(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long sunionstore(String dstkey, String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public String watch(final String... keys) {
    	checkIsInMulti(keys[0]);
		Jedis j = getShard(keys[0]);
		return j.watch(keys);
    }
	
	@Override
	public String unwatch() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String unwatch(String key) {
		checkIsInMulti(key);
		Jedis j = getShard(key);
		return j.unwatch();
	}

	@Override
	public Long zinterstore(String dstkey, String... sets) {
		checkIsInMulti(sets[0]);
		Jedis j = getShard(sets[0]);
		return j.zinterstore(dstkey, sets);
	}
	

	@Override
	public Long zinterstore(String dstkey, ZParams params, String... sets) {
		checkIsInMulti(sets[0]);
		Jedis j = getShard(sets[0]);
		return j.zinterstore(dstkey, params, sets);		
	}

	@Override
	public Long zunionstore(String dstkey, String... sets) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long zunionstore(String dstkey, ZParams params, String... sets) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String brpoplpush(String source, String destination, int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long publish(String channel, String message) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void subscribe(JedisPubSub jedisPubSub, String... channels) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String randomKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long bitop(BitOP op, String destKey, String... srcKeys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScanResult<String> scan(int cursor) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScanResult<String> scan(String cursor) {
		// TODO Auto-generated method stub
		return null;
	}
    
 
	protected void checkIsInMulti(String key) {
		Client client = getShard(key).getClient();
		if (client.isInMulti()) {
			throw new JedisDataException(
					"Cannot use Jedis when in Multi. Please use Jedis Transaction instead.");
		}
	}

    public List<String> flushAll() {
    	
    	List<String> status = new ArrayList<String>();
    	for (Jedis jedis : getAllShards()) {
          jedis.flushAll();
          //TODO get status reply from the shard - client.getStatusCodeReply();
          
    	}

    	return status;
    }

    
}
