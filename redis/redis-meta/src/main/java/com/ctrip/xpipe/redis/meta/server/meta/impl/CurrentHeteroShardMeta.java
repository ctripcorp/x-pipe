package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author ayq
 * <p>
 * 2022/4/6 21:02
 */
public class CurrentHeteroShardMeta extends AbstractCurrentShardMeta {

    private CurrentKeeperShardMeta keeperShardMeta;
    private CurrentApplierShardMeta applierShardMeta;

    public CurrentHeteroShardMeta(@JsonProperty("clusterDbId") Long clusterDbId, @JsonProperty("shardDbId") Long shardDbId,
                                  CurrentKeeperShardMeta keeperShardMeta, CurrentApplierShardMeta applierShardMeta) {
        super(clusterDbId, shardDbId);
        this.keeperShardMeta = keeperShardMeta;
        this.applierShardMeta = applierShardMeta;
    }

    public CurrentKeeperShardMeta getKeeperShardMeta() {
        return keeperShardMeta;
    }

    public CurrentApplierShardMeta getApplierShardMeta() {
        return applierShardMeta;
    }
}
