package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/**
 * @author ayq
 * <p>
 * 2022/4/6 21:02
 */
public class CurrentHeteroShardMeta extends AbstractCurrentShardMeta {

    private CurrentKeeperShardMeta keeperShardMeta;
    private CurrentApplierShardMeta applierShardMeta;
    private List<RedisMeta> redisMetas;

    @SuppressWarnings("unchecked")
    public CurrentHeteroShardMeta(@JsonProperty("clusterDbId") Long clusterDbId, @JsonProperty("shardDbId") Long shardDbId,
                                  @JsonProperty("keeperShardMeta") CurrentKeeperShardMeta keeperShardMeta,
                                  @JsonProperty("applierShardMeta") CurrentApplierShardMeta applierShardMeta,
                                  @JsonProperty("redisMetas") List<RedisMeta> redisMetas) {
        super(clusterDbId, shardDbId);
        this.keeperShardMeta = keeperShardMeta;
        this.applierShardMeta = applierShardMeta;
        this.redisMetas = redisMetas;
    }

    public CurrentKeeperShardMeta getKeeperShardMeta() {
        return keeperShardMeta;
    }

    public CurrentApplierShardMeta getApplierShardMeta() {
        return applierShardMeta;
    }

    public List<RedisMeta> getRedisMetas() {
        return redisMetas;
    }

    public String getSids(){

        logger.debug("[getSids]{}, {}", clusterDbId, shardDbId);

        StringBuilder result = new StringBuilder();
        if (redisMetas == null) {
            return result.toString();
        }

        Set<String> sidSet = new HashSet<>();
        for (RedisMeta redis : redisMetas) {
            if (redis.getSid() == null) {
                continue;
            }
            String[] sids = redis.getSid().split(",");
            sidSet.addAll(Arrays.asList(sids));
        }
        for (String sid : sidSet) {
            if (result.length() != 0) {
                result.append(",");
            }
            result.append(sid);
        }
        return result.toString();
    }

}
