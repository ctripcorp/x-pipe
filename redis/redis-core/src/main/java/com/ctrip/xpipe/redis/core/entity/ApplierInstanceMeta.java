package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;

/**
 * @author ayq
 * <p>
 * 2022/4/2 12:14
 */
public class ApplierInstanceMeta extends ApplierTransMeta {
    public ApplierInstanceMeta(){

    }

    public ApplierInstanceMeta(ClusterId clusterId, ShardId shardId, ApplierMeta applierMeta) {
        super(clusterId.id(), shardId.id(), applierMeta);
    }
}
