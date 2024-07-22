package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.core.metaserver.model.ShardAllMetaModel;

public interface MetaServerSlotService {

    ShardAllMetaModel getShardAllMeta(String dcId, String clusterId, String shardId);

    ShardAllMetaModel getLocalDcShardAllMeta(String dcId, String clusterId, String shardId);

}
