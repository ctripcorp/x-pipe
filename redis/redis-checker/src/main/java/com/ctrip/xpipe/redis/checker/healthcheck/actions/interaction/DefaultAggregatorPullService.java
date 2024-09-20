package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.migration.OuterClientService.*;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;


@Component
public class DefaultAggregatorPullService implements AggregatorPullService{

    @Autowired
    private RemoteCheckerManager remoteCheckerManager;
    @Autowired
    private AlertManager alertManager;
    @Autowired
    private CheckerConfig checkerConfig;
    @Autowired
    private MetaCache metaCache;
    private OuterClientService outerClientService = OuterClientService.DEFAULT;
    private static final Logger logger =  LoggerFactory.getLogger(DefaultAggregatorPullService.class);

    @Override
    public Set<HostPortDcStatus> getNeedAdjustInstances(Set<HostPort> instances) throws Exception{
        Set<HostPortDcStatus> instanceNeedAdjust = new HashSet<>();
        Map<HostPort, Boolean> xpipeAllHealthStatus = getXpipeAllHealthStatus(instances);
        logger.debug("[DefaultAggregatorPullService][getNeedAdjustInstances]xpipeAllHealthStatus:{}", xpipeAllHealthStatus);
        Map<HostPort, Boolean> outerClientAllHealthStatus = getOuterClientAllHealthStatus(instances);
        logger.debug("[DefaultAggregatorPullService][getNeedAdjustInstances]outerClientAllHealthStatus:{}", outerClientAllHealthStatus);
        for (Map.Entry<HostPort, Boolean> entry : xpipeAllHealthStatus.entrySet()) {
            if (!outerClientAllHealthStatus.containsKey(entry.getKey()) || !entry.getValue().equals(outerClientAllHealthStatus.get(entry.getKey()))) {
                instanceNeedAdjust.add(
                        new HostPortDcStatus(entry.getKey().getHost(), entry.getKey().getPort(), metaCache.getDc(entry.getKey()), entry.getValue()));
            }
        }
        return instanceNeedAdjust;
    }

    @Override
    public void doMarkInstances(String clusterName, Set<HostPortDcStatus> instances) throws OuterClientException {
        alertMarkInstance(clusterName, instances);
        MarkInstanceRequest markInstanceRequest = new MarkInstanceRequest(instances, clusterName, metaCache.getActiveDc(clusterName));
        outerClientService.batchMarkInstance(markInstanceRequest);
    }

    public Map<HostPort, Boolean> getXpipeAllHealthStatus(Set<HostPort> instances) {
        XPipeInstanceHealthHolder xPipeInstanceHealthHolder = new XPipeInstanceHealthHolder();
        for (CheckerService checkerService : remoteCheckerManager.getAllCheckerServices()) {
            xPipeInstanceHealthHolder.add(checkerService.getAllClusterInstanceHealthStatus(instances));
        }
        return xPipeInstanceHealthHolder.getAllHealthStatus(checkerConfig.getQuorum());
    }

    public Map<HostPort, Boolean> getOuterClientAllHealthStatus(Set<HostPort> hostPorts) throws Exception {
        Map<ClusterShardHostPort, Boolean> instancesUp = new HashMap<>();
        for (HostPort hostPort : hostPorts) {
            ClusterShardHostPort clusterShardHostPort = new ClusterShardHostPort(null, null, hostPort);
            instancesUp.put(clusterShardHostPort, outerClientService.isInstanceUp(clusterShardHostPort));
        }
        Map<HostPort, Boolean> result = new HashMap<>();
        for (Map.Entry<ClusterShardHostPort, Boolean> entry : instancesUp.entrySet()) {
            result.put(entry.getKey().getHostPort(), entry.getValue());
        }
        return result;
    }

    public void alertMarkInstance(String clusterName, Set<HostPortDcStatus> instances) {
        if (!instances.isEmpty()) {
            for (HostPortDcStatus instance : instances) {
                if (instance.isCanRead()) {
                    alertManager.alert(clusterName, null,
                            new HostPort(instance.getHost(), instance.getPort()), ALERT_TYPE.MARK_INSTANCE_UP, "Mark Instance Up");
                } else {
                    alertManager.alert(clusterName, null,
                            new HostPort(instance.getHost(), instance.getPort()), ALERT_TYPE.MARK_INSTANCE_DOWN, "Mark Instance Down");
                }
            }
        }
    }
}
