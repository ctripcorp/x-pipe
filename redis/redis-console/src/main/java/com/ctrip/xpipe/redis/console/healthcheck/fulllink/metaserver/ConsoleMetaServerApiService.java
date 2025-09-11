package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.model.ClusterDebugInfo;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardCurrentMetaModel;

public interface ConsoleMetaServerApiService {

    String PATH_SHARD_ALL_MATA = "/api/meta/all/{clusterName}/{shardName}";

    String PATH_MATA_CLUSTER_DEBUG = "/api/metacluster/debug";

    ShardCurrentMetaModel getShardAllMeta(HostPort metaServer, String clusterName, String shardName);

    ClusterDebugInfo getAllClusterSlotStateInfos();

}
