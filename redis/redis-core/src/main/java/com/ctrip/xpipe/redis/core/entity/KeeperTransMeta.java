package com.ctrip.xpipe.redis.core.entity;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * used for trans info between meta server and keeper container
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public class KeeperTransMeta implements ClusterAware{
	
	private String clusterId;

	private String shardId;
	
	private KeeperMeta keeperMeta;

	//for json conversion
	public KeeperTransMeta() {}
	
	public KeeperTransMeta(String clusterId, String shardId, KeeperMeta keeperMeta){
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.keeperMeta = keeperMeta;
	}
	

	@Override
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

	public KeeperMeta getKeeperMeta() {
		return keeperMeta;
	}

	public void setKeeperMeta(KeeperMeta keeperMeta) {
		this.keeperMeta = keeperMeta;
	}

	@Override
	public boolean equals(Object obj) {

		if(!(obj instanceof KeeperTransMeta)){
			return false;
		}
		
		KeeperTransMeta other =  (KeeperTransMeta) obj;
		if(!ObjectUtils.equals(this.clusterId, other.clusterId)){
			return false;
		}
		if(!ObjectUtils.equals(this.shardId, other.shardId)){
			return false;
		}
		if(!ObjectUtils.equals(this.keeperMeta, other.keeperMeta)){
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		
		int hash = 0;
		hash = hash * 31 + (clusterId == null ? 0 : clusterId.hashCode());
		hash = hash * 31 + (shardId == null ? 0 : shardId.hashCode());
		hash = hash * 31 + (keeperMeta == null ? 0 : keeperMeta.hashCode());
		return hash;
	}
	
	@Override
	public String toString() {
		return Codec.DEFAULT.encode(this);
	}
}
