package com.ctrip.xpipe.redis.console.health.clientconfig;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.utils.ServicesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
@Component
public class ClientConfigMonitor extends AbstractIntervalCheck{

    private OuterClientService outerClientService = ServicesUtil.getOuterClientService();

    @Autowired
    private MetaCache metaCache;

    @Override
    protected void doCheck() {

        logger.info("[doCheck]");

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();

        Set<String> clusters = getClusters(xpipeMeta);

        for(String cluster : clusters){
            try {
                checkCluster(cluster, xpipeMeta);
            } catch (Exception e) {
                logger.info("[doCheck]" + cluster, e);
            }
        }
    }

    private void checkCluster(String clusterName, XpipeMeta xpipeMeta) throws Exception {

        OuterClientService.ClusterInfo clusterInfo = outerClientService.getClusterInfo(clusterName);
        try{
            clusterInfo.check();
        }catch (Exception e){
            EventMonitor.DEFAULT.logAlertEvent(e.getMessage());
        }

        CheckCluster checkClusterInfo = fromInfo(clusterInfo, clusterName);
        CheckCluster checkClusterXPipe = fromXPipe(xpipeMeta, clusterName);

        try{
            checkClusterInfo.equals(checkClusterXPipe);
        }catch (EqualsException e){
            logger.warn("[checkCluster]", e);
            if(consoleConfig.alertClientConfigConsistent()){
                EventMonitor.DEFAULT.logAlertEvent("INCONSIS:" + e.simpleMessage());
            }
        }

    }

    private CheckCluster fromInfo(OuterClientService.ClusterInfo clusterInfo, String checkCluster) {

        CheckCluster result = new CheckCluster(checkCluster);
        clusterInfo.getGroups().forEach(groupMeta -> {

            CheckShard shard = result.getOrCreate(groupMeta.getName());
            groupMeta.getInstances().forEach(instance -> {
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

        if(dcMetas.length == 0){
            return new HashSet<>();
        }
        return new HashSet<>(dcMetas[0].getClusters().keySet());
    }

}
