package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentShardMeta;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractCurrentShardMeta implements CurrentShardMeta {

    @JsonIgnore
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @JsonIgnore
    private List<Releasable> resources = new LinkedList<>();

    protected Long clusterDbId, shardDbId;

    public AbstractCurrentShardMeta() {}

    public AbstractCurrentShardMeta(Long clusterDbId, Long shardDbId) {
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
    }

    public void addResource(Releasable releasable) {
        synchronized (resources) {
            resources.add(releasable);
        }
    }

    @Override
    public void release() throws Exception {
        logger.info("[release]cluster_{},shard_{}", clusterDbId, shardDbId);
        for (Releasable resource : resources) {
            try {
                resource.release();
            } catch (Exception e) {
                logger.error("[release]" + resource, e);
            }
        }
    }

    public Long getClusterDbId() {
        return clusterDbId;
    }

    public Long getShardDbId() {
        return shardDbId;
    }

    public boolean watchIfNotWatched() {
        return false;
    }

}
