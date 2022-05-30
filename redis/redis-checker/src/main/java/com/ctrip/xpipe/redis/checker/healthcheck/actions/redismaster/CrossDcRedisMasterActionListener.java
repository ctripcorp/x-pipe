package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.healthcheck.CrossDcSupport;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CrossDcRedisMasterActionListener extends OuterClientRedisMasterActionListener implements CrossDcSupport {


    @Autowired
    public CrossDcRedisMasterActionListener(PersistenceCache persistenceCache, MetaCache metaCache) {
        super(persistenceCache, metaCache);
    }

    @Override
    protected List<HostPort> findMasterInDcClusterShard(String dcId, String clusterId, String shardId) {
        List<HostPort> masters = new ArrayList<>();

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return masters;


        xpipeMeta.getDcs().values().forEach(dcMeta->{
            ClusterMeta clusterMeta = dcMeta.findCluster(clusterId);
            if (null == clusterMeta)
                return;

            ShardMeta shardMeta = clusterMeta.findShard(shardId);
            if (null == shardMeta)
                return;

            shardMeta.getRedises().forEach(redisMeta -> {
                if (redisMeta.isMaster()) masters.add(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
            });
        });


        return masters;
    }

    @Override
    protected RedisMeta finalMaster(String dcId, String clusterId, String shardId) {
        List<OuterClientService.InstanceInfo> shardMasters = new ArrayList<>();
        try {
            OuterClientService.ClusterInfo clusterInfo = outerClientService.getClusterInfo(clusterId);
            clusterInfo.getGroups().forEach(groupInfo -> {
                if (groupInfo.getName().equalsIgnoreCase(shardId)) {
                    groupInfo.getInstances().forEach(instanceInfo -> {
                        if (instanceInfo.isMaster() && instanceInfo.isStatus()) {
                            shardMasters.add(instanceInfo);
                        }
                    });
                }
            });
        } catch (Exception e) {
            throw new XpipeRuntimeException(String.format("get cluster info %s failed", clusterId), e);
        }
        if (shardMasters.isEmpty())
            throw new XpipeRuntimeException(String.format("no active master found in cross dc cluster shard: %s %s", clusterId, shardId));

        if (shardMasters.size() > 1)
            throw new XpipeRuntimeException(String.format("too many active masters found in cross dc cluster shard: %s %s", clusterId, shardId));

        OuterClientService.InstanceInfo master = shardMasters.get(0);
        return new RedisMeta().setIp(master.getIPAddress()).setPort(master.getPort());
    }

}
