package redis.clients.jedis.shardedcluster;

import java.util.List;

public class ClusterConfig {

	private String clustername;
	private int weight;
	private List<Node> nodes;
	
	public String getClustername() {
		return clustername;
	}
	public void setClustername(String clustername) {
		this.clustername = clustername;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	public List<Node> getNodes() {
		return nodes;
	}
	public void setNodes(List<Node> nodes) {
		this.nodes = nodes;
	}
	
	
}
