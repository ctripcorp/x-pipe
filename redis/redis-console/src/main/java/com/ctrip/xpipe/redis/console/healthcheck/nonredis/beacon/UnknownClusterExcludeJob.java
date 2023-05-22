package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.console.migration.auto.BeaconSystem;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/17
 */
public class UnknownClusterExcludeJob extends AbstractCommand<Set<String>> {

    private Set<String> expectClusters;

    private MonitorService monitorService;

    private int maxExcludeClusters;

    public UnknownClusterExcludeJob(Set<String> expectClusters, MonitorService monitorService, int maxExcludeClusters) {
        this.expectClusters = expectClusters;
        this.monitorService = monitorService;
        this.maxExcludeClusters = maxExcludeClusters;
    }

    @Override
    protected void doExecute() throws Throwable {
        Set<String> realClusters = monitorService.fetchAllClusters(BeaconSystem.getDefault().getSystemName());
        Set<String> needExcludeClusters = new HashSet<>(realClusters);

        needExcludeClusters.removeAll(expectClusters);
        if (needExcludeClusters.size() > maxExcludeClusters) {
            getLogger().info("[doExecute][{}] need exclude clusters too many, {}", monitorService, needExcludeClusters);
            future().setFailure(new TooManyNeedExcludeClusterException(needExcludeClusters));
            return;
        }

        Set<String> excludeClusters = new HashSet<>();
        for (String cluster: needExcludeClusters) {
            try {
                monitorService.unregisterCluster(BeaconSystem.getDefault().getSystemName(), cluster);
                excludeClusters.add(cluster);
            } catch (Throwable th) {
                getLogger().info("[doExecute][{}] unregister cluster {} fail", monitorService, cluster, th);
            }
        }

        future().setSuccess(excludeClusters);
    }

    @Override
    protected void doReset() {
    }

    @Override
    public String getName() {
        return "[UnknownClusterExcludeJob] " + monitorService;
    }
}
