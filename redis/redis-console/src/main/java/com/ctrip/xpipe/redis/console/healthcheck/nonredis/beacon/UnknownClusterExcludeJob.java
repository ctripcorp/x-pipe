package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/17
 */
public class UnknownClusterExcludeJob extends AbstractCommand<Set<String>> {

    private BeaconSystem beaconSystem;

    private Set<String> expectClusters;

    private List<MonitorService> monitorServices;

    private int maxExcludeClusters;

    public UnknownClusterExcludeJob(Set<String> expectClusters, List<MonitorService> monitorServices, int maxExcludeClusters) {
        this(BeaconSystem.getDefault(), expectClusters, monitorServices, maxExcludeClusters);
    }

    public UnknownClusterExcludeJob(BeaconSystem beaconSystem, Set<String> expectClusters, List<MonitorService> monitorServices, int maxExcludeClusters) {
        this.beaconSystem = beaconSystem;
        this.expectClusters = expectClusters;
        this.monitorServices = monitorServices;
        this.maxExcludeClusters = maxExcludeClusters;
    }

    @Override
    protected void doExecute() throws Throwable {
        Set<String> realClusters = new HashSet<>();
        Map<MonitorService, Set<String>> serviceClustersMap = new HashMap<>();
        String system = beaconSystem.getSystemName();
        monitorServices.forEach(ms -> {
            Set<String> clusters = ms.fetchAllClusters(system);
            serviceClustersMap.put(ms, clusters);
            realClusters.addAll(clusters);
        });
        Set<String> needExcludeClusters = new HashSet<>(realClusters);

        needExcludeClusters.removeAll(expectClusters);
        if (needExcludeClusters.size() > maxExcludeClusters) {
            getLogger().warn("[doExecute][{}] need exclude clusters too many, {}", monitorServices, needExcludeClusters);
            future().setFailure(new TooManyNeedExcludeClusterException(needExcludeClusters));
            return;
        }

        Set<String> excludeClusters = new HashSet<>();
        for (String cluster: needExcludeClusters) {
            try {
                serviceClustersMap.forEach((ms, clusters) -> {
                    if (clusters.contains(cluster)) {
                        ms.unregisterCluster(system, cluster);
                    }
                });
                excludeClusters.add(cluster);
            } catch (Throwable th) {
                getLogger().info("[doExecute][{}] unregister cluster {} fail", monitorServices, cluster, th);
            }
        }

        future().setSuccess(excludeClusters);
    }

    @Override
    protected void doReset() {
    }

    @Override
    public String getName() {
        return "[UnknownClusterExcludeJob] " + monitorServices;
    }
}
