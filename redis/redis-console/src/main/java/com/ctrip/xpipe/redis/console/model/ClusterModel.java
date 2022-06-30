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
	/* do not use both version1 and version2 at the same time
	 * TODO song_yu remove code of version1
	 * */

	/* version 1*/
	private List<DcTbl> dcs;
	private List<ShardModel> shards;

	/* version 2*/
	private List<ReplDirectionInfoModel> replDirections;
	private List<DcClusterModel> dcClusters;
	
	public ClusterModel() {
		
	}
	
	public ClusterTbl getClusterTbl() {
		return this.clusterTbl;
	}
	
	public ClusterModel setClusterTbl(ClusterTbl clusterTbl) {
		this.clusterTbl = clusterTbl;
		return this;
	}

	public List<DcTbl> getDcs() {
		return dcs;
	}

	public void setDcs(List<DcTbl> dcs) {
		this.dcs = dcs;
	}

	public List<ShardModel> getShards() {
		return shards;
	}

	public void setShards(List<ShardModel> shards) {
		this.shards = shards;
	}

	public List<ReplDirectionInfoModel> getReplDirections() {
		return replDirections;
	}

	public ClusterModel setReplDirections(List<ReplDirectionInfoModel> replDirections) {
		this.replDirections = replDirections;
		return this;
	}

	public List<DcClusterModel> getDcClusters() {
		return dcClusters;
	}

	public ClusterModel setDcClusters(List<DcClusterModel> dcClusters) {
		this.dcClusters = dcClusters;
		return this;
	}
}
