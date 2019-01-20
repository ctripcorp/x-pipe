package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@ConditionalOnProperty(name = {HealthChecker.ENABLED}, matchIfMissing = true)
public class ClientConfigMonitor4MasterStatus extends AbstractClientConfigMonitor {

    private static final long checkIntervalMill = Long.parseLong(System.getProperty("console.outerclient.check.interval", "30000"));

    @Autowired(required = false)
    private ConsoleLeaderElector consoleSiteLeader;

    @Override
    protected boolean shouldCheck() {
        if(consoleSiteLeader != null && !consoleSiteLeader.amILeader()) {
            logger.debug("[shouldCheck][not local dc leader, quit]");
            return false;
        }
        return true;
    }

    @Autowired
    private ClusterHealthMonitorManager clusterHealthMonitorManager;

    private Set<String> warningShards = Sets.newConcurrentHashSet();

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    protected long getIntervalMilli() {
        return checkIntervalMill;
    }

    @Override
    protected void checkShard(OuterClientService.GroupInfo group, String clusterName) {
        String shardName = group.getName();
        boolean shardMasterWarn = false;
        for(OuterClientService.InstanceInfo instance : group.getInstances()) {

            if(!instance.isStatus() && instance.isMaster()) {
                clusterHealthMonitorManager.outerClientMasterDown(clusterName, shardName);
                warningShards.add(shardName);
                shardMasterWarn = true;
            }
        }
        if(!shardMasterWarn && warningShards.contains(shardName)) {
            warningShards.remove(shardName);
            clusterHealthMonitorManager.outerClientMasterUp(clusterName, shardName);
        }
    }
}
