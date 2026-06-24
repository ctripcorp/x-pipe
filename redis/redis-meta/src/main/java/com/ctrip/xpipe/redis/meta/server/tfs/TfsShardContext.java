package com.ctrip.xpipe.redis.meta.server.tfs;

/**
 * Cluster/shard identity for TFS command log correlation.
 */
public final class TfsShardContext {

    private final Long clusterDbId;
    private final Long shardDbId;

    public TfsShardContext(Long clusterDbId, Long shardDbId) {
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
    }

    public Long getClusterDbId() {
        return clusterDbId;
    }

    public Long getShardDbId() {
        return shardDbId;
    }
}
