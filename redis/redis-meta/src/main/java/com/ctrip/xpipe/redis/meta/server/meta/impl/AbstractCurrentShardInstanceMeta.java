package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.meta.server.meta.CurrentShardInstanceMeta;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ayq
 * <p>
 * 2022/10/20 11:15
 */
public class AbstractCurrentShardInstanceMeta implements CurrentShardInstanceMeta {

    @JsonIgnore
    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected Long clusterDbId, shardDbId;

    public AbstractCurrentShardInstanceMeta() {}

    public AbstractCurrentShardInstanceMeta(Long clusterDbId, Long shardDbId) {
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
    }

    public boolean watchIfNotWatched() {
        return false;
    }
}
