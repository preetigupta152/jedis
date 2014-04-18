package redis.clients.jedis.shardedcluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import redis.clients.jedis.exceptions.JedisException;

/**
 * Reads yaml JedisShard configuration
 * 
 * @author ngovindasamy
 * 
 */
public class JedisShardConfigReader {

	//public final static String YAML_CONFIG_FILE = "jedis-cluster-config.yml";

	@SuppressWarnings("unchecked")
	public ClusterConfig loadConfig(File file) {

		Yaml yaml = new Yaml();
		ClusterConfig cfg = null;

		try {
			InputStream in = new FileInputStream(file);
			cfg = yaml.loadAs(in, ClusterConfig.class);

		} catch (FileNotFoundException e) {
			throw new JedisException("YAML Config File not found");
		}
		return cfg;
	}
}
