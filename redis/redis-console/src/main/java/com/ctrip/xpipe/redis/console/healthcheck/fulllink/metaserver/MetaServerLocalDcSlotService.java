package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardCurrentMetaModel;

public interface MetaServerLocalDcSlotService {

    CommandFuture<ShardCurrentMetaModel> getLocalDcShardCurrentMeta(long clusterId, String clusterName, String shardName);

    HostPort getLocalDcManagerMetaServer(long clusterId);

}
