package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.healthcheck.LocalDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.SingleDcSupport;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

@Component
public class OuterClientRedisMasterActionListener extends AbstractRedisMasterActionListener implements SingleDcSupport, LocalDcSupport {

    protected OuterClientService outerClientService = OuterClientService.DEFAULT;


    @Autowired
    public OuterClientRedisMasterActionListener(PersistenceCache persistenceCache, MetaCache metaCache) {
        super(persistenceCache, metaCache, Executors.newFixedThreadPool(100, XpipeThreadFactory.create("OuterClientRedisMasterJudgement")));
    }


    @Override
    protected RedisMeta finalMaster(String dcId, String clusterId, String shardId) throws Exception {
        OuterClientService.ClusterInfo clusterInfo = outerClientService.getClusterInfo(clusterId);
        List<OuterClientService.InstanceInfo> shardMasters = new ArrayList<>();
        clusterInfo.getGroups().forEach(groupInfo -> {
            if (groupInfo.getName().equalsIgnoreCase(shardId)) {
                groupInfo.getInstances().forEach(instanceInfo -> {
                    if (instanceInfo.isMaster() && instanceInfo.isStatus() && instanceInfo.getEnv().equalsIgnoreCase(dcId)) {
                        shardMasters.add(instanceInfo);
                    }
                });
            }
        });

        if (shardMasters.isEmpty())
            throw new OuterClientException(String.format("no active master found in dc cluster shard: %s %s %s",dcId, clusterId, shardId));

        if (shardMasters.size() > 1)
            throw new OuterClientException(String.format("too many active masters found in dc cluster shard:%s %s %s",dcId, clusterId, shardId));

        OuterClientService.InstanceInfo master = shardMasters.get(0);
        return new RedisMeta().setIp(master.getIPAddress()).setPort(master.getPort());

    }

    @Override
    protected String getServerName() {
        return "outer client";
    }
}
