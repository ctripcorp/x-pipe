package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public class KeeperKey {
	
	private String dc;
	
	private String clusterId;
	
	private String shardId;
	
	public KeeperKey(){
	}
	
	public KeeperKey(String dc, String clusterId, String shardId){
		this.dc = dc;
		this.clusterId = clusterId;
		this.shardId = shardId;
	}
	

	public String getClusterId() {
		return clusterId;
	}
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}
	public String getShardId() {
		return shardId;
	}
	public void setShardId(String shardId) {
		this.shardId = shardId;
	}

	public void setDc(String dc) {
		this.dc = dc;
	}
	public String getDc() {
		return dc;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof KeeperKey)){
			return false;
		}
		KeeperKey other = (KeeperKey) obj;
		if(!ObjectUtils.equals(this.dc, other.dc)){
			return false;
		}
		
		if(!ObjectUtils.equals(this.clusterId, other.clusterId)){
			return false;
		}

		if(!ObjectUtils.equals(this.shardId, other.shardId)){
			return false;
		}

		return true;
	}
	
	@Override
	public int hashCode() {
		
		int hash = 0;
		hash = hash*31 + (this.dc == null ? 0 : this.dc.hashCode());
		hash = hash*31 + (this.clusterId == null ? 0 : this.clusterId.hashCode());
		hash = hash*31 + (this.shardId == null ? 0 : this.shardId.hashCode());
		return hash;
	}
}
