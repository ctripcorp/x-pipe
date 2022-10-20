package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/**
 * @author ayq
 * <p>
 * 2022/4/6 21:02
 */
public class CurrentOneWayShardMeta extends AbstractCurrentShardMeta {

    private CurrentShardKeeperMeta shardKeeperMeta;
    private CurrentShardApplierMeta shardApplierMeta;
    private List<RedisMeta> redisMetas;

    @SuppressWarnings("unchecked")
    public CurrentOneWayShardMeta(@JsonProperty("clusterDbId") Long clusterDbId, @JsonProperty("shardDbId") Long shardDbId,
                                  @JsonProperty("shardKeeperMeta") CurrentShardKeeperMeta shardKeeperMeta,
                                  @JsonProperty("shardApplierMeta") CurrentShardApplierMeta shardApplierMeta,
                                  @JsonProperty("redisMetas") List<RedisMeta> redisMetas) {
        super(clusterDbId, shardDbId);
        this.shardKeeperMeta = shardKeeperMeta;
        this.shardApplierMeta = shardApplierMeta;
        this.redisMetas = redisMetas;
    }

    public CurrentShardKeeperMeta getShardKeeperMeta() {
        return shardKeeperMeta;
    }

    public CurrentShardApplierMeta getShardApplierMeta() {
        return shardApplierMeta;
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
