package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractIntervalCheck;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */

public class AbstractClientConfigMonitor extends AbstractIntervalCheck {

    @Autowired
    private OuterClientCache outerClientCache;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    protected void doCheck() {

        logger.info("[doCheck]");

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();

        // check for local dc meta only
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getId().equalsIgnoreCase(currentDcId)) {
                continue;
            }
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (!clusterType.supportHealthCheck())
                    continue;
                if (clusterType.supportSingleActiveDC()
                        && !clusterMeta.getActiveDc().equalsIgnoreCase(currentDcId)) {
                    continue;
                }
                if (clusterType.supportMultiActiveDC()) {
                    // only check client config in the first dc for multi active dc cluster
//                    String[] dcs = clusterMeta.getDcs().split("\\s*,\\s*");
//                    if (dcs.length > 0 && !dcs[0].equalsIgnoreCase(currentDcId)) {
//                        continue;
//                    }
                    // TODO: recovery check after crdt redis stable
                    continue;
                }
                try {
                    checkCluster(clusterMeta.getId(), xpipeMeta);
                } catch (Exception e) {
                    logger.info("[doCheck][{}]{}" + clusterMeta.getId(), e);
                }

            }
        }
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.CLIENT_INCONSIS, ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
    }

    private void checkCluster(String clusterName, XpipeMeta xpipeMeta) throws Exception {

        OuterClientService.ClusterInfo clusterInfo = outerClientCache.getClusterInfo(clusterName);
        try {
            checkClusterInfo(clusterInfo);
        } catch (Exception e) {
            logger.error("[checkCluster] {}", e);
        }

        CheckCluster checkClusterInfo = fromInfo(clusterInfo, clusterName);
        CheckCluster checkClusterXPipe = fromXPipe(xpipeMeta, clusterName);

        try {
            checkClusterInfo.equals(checkClusterXPipe);
        } catch (EqualsException e) {
            logger.warn("[checkCluster]", e);
            alertManager.alert(clusterName, e.getShardName(), null, ALERT_TYPE.CLIENT_INCONSIS, e.getMessage());
        }

    }

    private void checkClusterInfo(OuterClientService.ClusterInfo clusterInfo) {
        if(clusterInfo.getGroups() == null || clusterInfo.getGroups().isEmpty())
            return;
        String clusterName = clusterInfo.getName();

        for(OuterClientService.GroupInfo group : clusterInfo.getGroups()) {
            List<OuterClientService.InstanceInfo> instances = group.getInstances();
            if(instances == null || instances.isEmpty())
                continue;
            checkShard(group, clusterName);
        }
    }

    protected void checkShard(OuterClientService.GroupInfo group, String clusterName) {
        String shardName = group.getName();
        boolean shardMasterWarn = false;
        for(OuterClientService.InstanceInfo instance : group.getInstances()) {
            if(!instance.isCanRead()) {
                alertManager.alert(clusterName, shardName,
                        new HostPort(instance.getIPAddress(), instance.getPort()),
                        ALERT_TYPE.CLIENT_INSTANCE_NOT_OK, "instance cannot read");
            }
            if(!instance.isStatus()) {
                alertManager.alert(clusterName, shardName,
                        new HostPort(instance.getIPAddress(), instance.getPort()),
                        ALERT_TYPE.CLIENT_INSTANCE_NOT_OK, "instance is not valid");
            }
        }
    }

    private CheckCluster fromInfo(OuterClientService.ClusterInfo clusterInfo, String checkCluster) {

        CheckCluster result = new CheckCluster(checkCluster);
        List<OuterClientService.GroupInfo> groups = clusterInfo.getGroups();
        if (groups == null) {
            return result;
        }

        groups.forEach(groupMeta -> {

            CheckShard shard = result.getOrCreate(groupMeta.getName());
            List<OuterClientService.InstanceInfo> instances = groupMeta.getInstances();
            if (instances == null) {
                return;
            }
            instances.forEach(instance -> {
                CheckRedis checkRedis = new CheckRedis(instance.getIPAddress(), instance.getPort(), instance.getEnv());
                shard.addRedis(checkRedis);
            });
        });

        return result;
    }

    private CheckCluster fromXPipe(XpipeMeta xpipeMeta, String checkCluster) {

        XpipeMetaManager xpm = new DefaultXpipeMetaManager(xpipeMeta);
        CheckCluster result = new CheckCluster(checkCluster);

        for (String dc : xpipeMeta.getDcs().keySet()) {

            ClusterMeta clusterMeta = xpm.getClusterMeta(dc, checkCluster);
            if (clusterMeta == null) {
                continue;
            }
            for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                CheckShard orShard = result.getOrCreate(shardMeta.getId());
                shardMeta.getRedises().forEach(redis -> {
                    orShard.addRedis(new CheckRedis(redis.getIp(), redis.getPort(), dc));
                });
            }
        }
        return result;
    }

}
