package redis.clients.jedis.shardedcluster;

/**
 * Cluster Node definition
 * @author ngovindasamy
 *
 */
public class Node {

	private String name;
	private String host;
	private int    port;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	
	public String getPortStr() {
		return Integer.toString(port);
	}
	
	public void setPort(int port) {
		this.port = port;
	}


}
