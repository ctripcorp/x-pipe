package com.ctrip.xpipe.redis.console.model;

import java.util.List;

/**
 * @author shyin
 *
 * Oct 21, 2016
 */
public class ClusterModel implements java.io.Serializable{
	private static final long serialVersionUID = 1L;
	
	private ClusterTbl clusterTbl;
	private List<DcTbl> slaveDcs;
	private List<ShardModel> shards;
	
	public ClusterModel() {
		
	}
	
	public ClusterTbl getClusterTbl() {
		return this.clusterTbl;
	}
	
	public ClusterModel setClusterTbl(ClusterTbl clusterTbl) {
		this.clusterTbl = clusterTbl;
		return this;
	}
	
	public List<DcTbl> getSlaveDcs() {
		return this.slaveDcs;
	}
	
	public ClusterModel setSlaveDcs(List<DcTbl> slaveDcs) {
		this.slaveDcs = slaveDcs;
		return this;
	}

	public List<ShardModel> getShards() {
		return shards;
	}

	public void setShards(List<ShardModel> shards) {
		this.shards = shards;
	}
	
}
