package com.ctrip.xpipe.redis.core.entity;


import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;

import java.util.Objects;

/**
 * used for trans info between meta server and keeper container
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public class KeeperTransMeta implements ClusterAware{
	
	private String clusterId;

	private String shardId;

	private Long clusterDbId;

	private Long shardDbId;
	
	private KeeperMeta keeperMeta;

	//for json conversion
	public KeeperTransMeta() {}
	
	public KeeperTransMeta(String clusterId, String shardId, KeeperMeta keeperMeta){
		this(clusterId, shardId, null, null, keeperMeta);
	}

	public KeeperTransMeta(String clusterId, String shardId, Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.clusterDbId = clusterDbId;
		this.shardDbId = shardDbId;
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

	public Long getClusterDbId() {
		return clusterDbId;
	}

	public void setClusterDbId(Long clusterDbId) {
		this.clusterDbId = clusterDbId;
	}

	public Long getShardDbId() {
		return shardDbId;
	}

	public void setShardDbId(Long shardDbId) {
		this.shardDbId = shardDbId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		KeeperTransMeta that = (KeeperTransMeta) o;
		return Objects.equals(clusterId, that.clusterId) &&
				Objects.equals(shardId, that.shardId) &&
				Objects.equals(clusterDbId, that.clusterDbId) &&
				Objects.equals(shardDbId, that.shardDbId) &&
				Objects.equals(keeperMeta, that.keeperMeta);
	}

	@Override
	public int hashCode() {
		return Objects.hash(clusterId, shardId, clusterDbId, shardDbId, keeperMeta);
	}
	
	@Override
	public String toString() {
		return String.format("[%s,%s(%d-%d)-%s:%d]", clusterId, shardId, clusterDbId, shardDbId, keeperMeta.getIp(), keeperMeta.getPort());
	}
}
