package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractSiteLeaderIntervalCheck;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
@Component
@ConditionalOnProperty(name = {HealthChecker.ENABLED}, matchIfMissing = true)
public class ClientConfigMonitor extends AbstractSiteLeaderIntervalCheck {

    private static final long checkIntervalMill = Long.parseLong(System.getProperty("console.outerclient.check.interval", "30000"));

    private OuterClientService outerClientService = ServicesUtil.getOuterClientService();

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private ClusterHealthMonitorManager clusterHealthMonitorManager;

    @Override
    protected void doCheck() {

        logger.info("[doCheck]");

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();

        Set<String> clusters = getClusters(xpipeMeta);

        for (String cluster : clusters) {
            try {
                checkCluster(cluster, xpipeMeta);
            } catch (Exception e) {
                logger.info("[doCheck]" + cluster, e);
            }
        }
    }

    @Override
    protected long getIntervalMilli() {
        return checkIntervalMill;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.CLIENT_INCONSIS, ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
    }

    private void checkCluster(String clusterName, XpipeMeta xpipeMeta) throws Exception {

        OuterClientService.ClusterInfo clusterInfo = outerClientService.getClusterInfo(clusterName);
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
        String clusterName = clusterInfo.getName(), shardName;

        for(OuterClientService.GroupInfo group : clusterInfo.getGroups()) {
            List<OuterClientService.InstanceInfo> instances = group.getInstances();
            if(instances == null || instances.isEmpty())
                continue;

            shardName = group.getName();
            boolean shardMasterWarn = false;
            for(OuterClientService.InstanceInfo instance : instances) {
                if(!instance.isCanRead()) {
                    alertManager.alert(clusterName, shardName,
                            new HostPort(instance.getIPAddress(), instance.getPort()),
                            ALERT_TYPE.CLIENT_INSTANCE_NOT_OK, "instance cannot read");
                }
                if(!instance.isStatus()) {
                    alertManager.alert(clusterName, shardName,
                            new HostPort(instance.getIPAddress(), instance.getPort()),
                            ALERT_TYPE.CLIENT_INSTANCE_NOT_OK, "instance is not valid");
                    if(instance.isMaster()) {
                        clusterHealthMonitorManager.outerClientMasterDown(clusterName, shardName);

                        shardMasterWarn = true;
                    }
                }
            }
            if(!shardMasterWarn) {
                clusterHealthMonitorManager.outerClientMasterUp(clusterName, shardName);
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


    public Set<String> getClusters(XpipeMeta xpipeMeta) {

        DcMeta[] dcMetas = xpipeMeta.getDcs().values().toArray(new DcMeta[0]);

        if (dcMetas.length == 0) {
            return new HashSet<>();
        }
        return new HashSet<>(dcMetas[0].getClusters().keySet());
    }

}
