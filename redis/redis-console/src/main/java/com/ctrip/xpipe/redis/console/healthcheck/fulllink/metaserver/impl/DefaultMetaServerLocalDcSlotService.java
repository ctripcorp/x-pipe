package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.ConsoleMetaServerApiService;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.MetaServerLocalDcSlotService;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.MetaServerManager;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.command.ShardCurrentMetaGetCommand;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardCurrentMetaModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

@Component
public class DefaultMetaServerLocalDcSlotService implements MetaServerLocalDcSlotService {

    @Autowired
    private MetaServerManager metaServerManager;

    @Autowired
    private ConsoleMetaServerApiService metaServerApiService;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executor;

    @Override
    public CommandFuture<ShardCurrentMetaModel> getLocalDcShardCurrentMeta(long clusterId, String clusterName, String shardName) {
        return new ShardCurrentMetaGetCommand(metaServerApiService, getLocalDcManagerMetaServer(clusterId), clusterName, shardName).execute(executor);
    }

    @Override
    public HostPort getLocalDcManagerMetaServer(long clusterId) {
        return metaServerManager.getLocalDcManagerMetaServer(clusterId);
    }

}
