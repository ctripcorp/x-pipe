package com.ctrip.xpipe.redis.core.entity;

import java.util.Objects;

/**
 * used for trans info between meta server and keeper container
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public class KeeperTransMeta {

	private Long clusterDbId;

	private Long shardDbId;

	private Long replId;
	
	private KeeperMeta keeperMeta;

	//for json conversion
	public KeeperTransMeta() {}

	public KeeperTransMeta(Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		this(clusterDbId, shardDbId, null, keeperMeta);
	}

	public KeeperTransMeta(Long replId, KeeperMeta keeperMeta) {
		this(null, null, replId, keeperMeta);
	}

	public KeeperTransMeta(Long clusterDbId, Long shardDbId, Long replId, KeeperMeta keeperMeta) {
		this.clusterDbId = clusterDbId;
		this.shardDbId = shardDbId;
		this.replId = replId;
		this.keeperMeta = keeperMeta;
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

	public Long getReplId() {
		return replId;
	}

	public void setReplId(Long replId) {
		this.replId = replId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		KeeperTransMeta that = (KeeperTransMeta) o;
		return Objects.equals(clusterDbId, that.clusterDbId) &&
				Objects.equals(shardDbId, that.shardDbId) &&
				Objects.equals(replId, ((KeeperTransMeta) o).replId) &&
				Objects.equals(keeperMeta, that.keeperMeta);
	}

	@Override
	public int hashCode() {
		return Objects.hash(clusterDbId, shardDbId, replId, keeperMeta);
	}

	@Override
	public String toString() {
		return String.format("[%d,%d-%d-%s:%d]", clusterDbId, shardDbId, replId, keeperMeta.getIp(), keeperMeta.getPort());
	}
}
