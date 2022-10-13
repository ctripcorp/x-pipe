package com.ctrip.xpipe.redis.core.entity;

import java.util.Objects;

/**
 * used for trans info between meta server and keeper container
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public class KeeperTransMeta {

	public enum KeeperReplType {
		REPL_DEFAULT(false),
		REPL_HYTERO(true);

		private final boolean supportGtidSet;
		KeeperReplType(boolean supportGtidSet) {
			this.supportGtidSet = supportGtidSet;
		}

		public boolean supportGtidSet() {
			return supportGtidSet;
		}

		public static KeeperReplType defaultType() {
			return REPL_DEFAULT;
		}
	}

	private Long clusterDbId;

	private Long shardDbId;
	
	private KeeperMeta keeperMeta;

	private KeeperReplType keeperReplType = KeeperReplType.defaultType();

	//for json conversion
	public KeeperTransMeta() {}

	public KeeperTransMeta(Long clusterDbId, Long shardDbId, KeeperReplType keeperReplType, KeeperMeta keeperMeta) {
		this.clusterDbId = clusterDbId;
		this.shardDbId = shardDbId;
		this.keeperReplType = keeperReplType;
		this.keeperMeta = keeperMeta;
	}

	public KeeperTransMeta(Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		this(clusterDbId, shardDbId, KeeperReplType.defaultType(), keeperMeta);
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

	public KeeperReplType getKeeperReplType() {
		return keeperReplType;
	}

	public void setKeeperReplType(KeeperReplType keeperReplType) {
		this.keeperReplType = keeperReplType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		KeeperTransMeta that = (KeeperTransMeta) o;
		return Objects.equals(clusterDbId, that.clusterDbId) &&
				Objects.equals(shardDbId, that.shardDbId) &&
				Objects.equals(keeperMeta, that.keeperMeta) &&
				keeperReplType == that.keeperReplType;
	}

	@Override
	public int hashCode() {
		return Objects.hash(clusterDbId, shardDbId, keeperMeta, keeperReplType);
	}

	@Override
	public String toString() {
		return String.format("[%d,%d-%s:%d-%s]", clusterDbId, shardDbId, keeperMeta.getIp(), keeperMeta.getPort(), keeperReplType);
	}
}
