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

    protected String clusterId, shardId;

    public AbstractCurrentShardMeta() {}

    public AbstractCurrentShardMeta(String clusterId, String shardId) {
        this.clusterId = clusterId;
        this.shardId = shardId;
    }

    public void addResource(Releasable releasable) {
        synchronized (resources) {
            resources.add(releasable);
        }
    }

    @Override
    public void release() throws Exception {
        logger.info("[release]{},{}", clusterId, shardId);
        for (Releasable resource : resources) {
            try {
                resource.release();
            } catch (Exception e) {
                logger.error("[release]" + resource, e);
            }
        }
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getShardId() {
        return shardId;
    }

    public boolean watchIfNotWatched() {
        return false;
    }

}
