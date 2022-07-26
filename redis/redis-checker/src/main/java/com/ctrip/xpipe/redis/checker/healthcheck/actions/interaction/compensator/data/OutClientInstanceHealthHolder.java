package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.google.common.collect.Maps;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/7/21
 */
public class OutClientInstanceHealthHolder {

    private Map<String, OuterClientService.ClusterInfo> clusters;

    public OutClientInstanceHealthHolder() {
        this(Maps.newConcurrentMap());
    }

    public OutClientInstanceHealthHolder(Map<String, OuterClientService.ClusterInfo> outerClientClusters) {
        this.clusters = outerClientClusters;
    }

    public synchronized void addClusters(Map<String, OuterClientService.ClusterInfo> outerClientClusters) {
        this.clusters.putAll(outerClientClusters);
    }

    public UpDownInstances extractReadable(Map<String, Set<HostPort>> interested) {
        Set<HostPort> healthyInstances = new HashSet<>();
        Set<HostPort> unhealthyInstances = new HashSet<>();

        for (String cluster: interested.keySet()) {
            OuterClientService.ClusterInfo clusterInfo = clusters.get(cluster.toLowerCase());
            if (null == clusterInfo) continue;

            Set<HostPort> interestedInstances = interested.get(cluster);
            for (OuterClientService.GroupInfo group : clusterInfo.getGroups()) {
                group.getInstances().forEach(instanceInfo -> {
                    HostPort hostPort = new HostPort(instanceInfo.getIPAddress(), instanceInfo.getPort());
                    if (!interestedInstances.contains(hostPort)) return;
                    if (instanceInfo.isCanRead()) healthyInstances.add(hostPort);
                    else unhealthyInstances.add(hostPort);
                });
            }
        }

        return new UpDownInstances(healthyInstances, unhealthyInstances);
    }

}
