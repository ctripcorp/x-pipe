package com.ctrip.xpipe.redis.console.health.clientconfig;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.health.AbstractIntervalCheck;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.utils.ServicesUtil;
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
public class ClientConfigMonitor extends AbstractIntervalCheck {

    private OuterClientService outerClientService = ServicesUtil.getOuterClientService();

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AlertManager alertManager;

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

    private void checkCluster(String clusterName, XpipeMeta xpipeMeta) throws Exception {

        OuterClientService.ClusterInfo clusterInfo = outerClientService.getClusterInfo(clusterName);
        try {
            clusterInfo.check();
        } catch (Exception e) {
            alertManager.alert(clusterName, null, ALERT_TYPE.CLIENT_INSTANCE_NOT_OK, e.getMessage());
        }

        CheckCluster checkClusterInfo = fromInfo(clusterInfo, clusterName);
        CheckCluster checkClusterXPipe = fromXPipe(xpipeMeta, clusterName);

        try {
            checkClusterInfo.equals(checkClusterXPipe);
        } catch (EqualsException e) {
            logger.warn("[checkCluster]", e);
            alertManager.alert(clusterName, e.getShardName(), ALERT_TYPE.CLIENT_INCONSIS, e.getMessage());
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
