package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.ConsoleMetaServerApiService;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardCurrentMetaModel;

public class ShardCurrentMetaGetCommand extends AbstractCommand<ShardCurrentMetaModel> {

    ConsoleMetaServerApiService service;

    HostPort metaServer;

    String clusterName;

    String shardName;

    public ShardCurrentMetaGetCommand(ConsoleMetaServerApiService service, HostPort metaServer, String clusterName, String shardName) {
        this.service = service;
        this.metaServer = metaServer;
        this.clusterName = clusterName;
        this.shardName = shardName;
    }

    @Override
    public String getName() {
        return "ShardCurrentMetaGetCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        this.future().setSuccess(service.getShardAllMeta(metaServer, clusterName, shardName));
    }

    @Override
    protected void doReset() {

    }
}
