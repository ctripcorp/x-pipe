package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.ConsoleMetaServerApiService;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.model.ClusterDebugInfo;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardCurrentMetaModel;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DefaultConsoleMetaServerApiService extends AbstractService implements ConsoleMetaServerApiService {

    @Autowired
    private ConsoleConfig consoleConfig;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Override
    public ShardCurrentMetaModel getShardAllMeta(HostPort metaServer, String clusterName, String shardName) {
        return restTemplate.getForObject(getPath(metaServer, PATH_SHARD_ALL_MATA), ShardCurrentMetaModel.class, clusterName, shardName);
    }

    @Override
    public ClusterDebugInfo getAllClusterSlotStateInfos() {
        return restTemplate.getForObject(String.format("%s%s", consoleConfig.getMetaservers().get(currentDc), PATH_MATA_CLUSTER_DEBUG), ClusterDebugInfo.class);
    }


    private String getPath(HostPort key, String path) {
        return "http://" + key.getHost() + ":" + key.getPort() + path;
    }
}
