package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.MetaServerLocalDcSlotService;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.MetaServerSlotService;
import com.ctrip.xpipe.redis.core.metaserver.model.ShardAllMetaModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.concurrent.ExecutionException;

@Component
public class MetaServerSlotServiceImpl implements MetaServerSlotService {

    @Autowired
    public ConsoleServiceManager consoleManager;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private MetaServerLocalDcSlotService metaServerLocalDcSlotService;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Override
    public ShardAllMetaModel getShardAllMeta(String dcId, String clusterId, String shardId) {
        if (currentDc.equals(dcId.toUpperCase())) {
            return getLocalDcShardAllMeta(dcId, clusterId, shardId);
        }
        return consoleManager.getShardAllMeta(dcId, clusterId, shardId);
    }

    @Override
    public ShardAllMetaModel getLocalDcShardAllMeta(String dcId, String clusterId, String shardId) {
        ClusterTbl clusterTbl = clusterService.find(clusterId);
        if (clusterTbl == null) {
            return null;
        }
        ShardAllMetaModel model = new ShardAllMetaModel();
        try {
            model.setShardCurrentMeta(metaServerLocalDcSlotService.getLocalDcShardCurrentMeta(clusterTbl.getId(), clusterId, shardId).get());
        } catch (InterruptedException | ExecutionException e) {
            model.setErr(e);
        }

        HostPort metaServer = metaServerLocalDcSlotService.getLocalDcManagerMetaServer(clusterTbl.getId());
        model.setMetaHost(metaServer.getHost())
                .setMetaPort(metaServer.getPort());
        return model;
    }
}
