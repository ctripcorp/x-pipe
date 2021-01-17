package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.beacon.BeaconService;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/17
 */
public class UnknownClusterExcludeJob extends AbstractCommand<Set<String>> {

    private Set<String> expectClusters;

    private BeaconService beaconService;

    private int maxExcludeClusters;

    public UnknownClusterExcludeJob(Set<String> expectClusters, BeaconService beaconService, int maxExcludeClusters) {
        this.expectClusters = expectClusters;
        this.beaconService = beaconService;
        this.maxExcludeClusters = maxExcludeClusters;
    }

    @Override
    protected void doExecute() throws Throwable {
        Set<String> realClusters = beaconService.fetchAllClusters();
        Set<String> needExcludeClusters = new HashSet<>(realClusters);

        needExcludeClusters.removeAll(expectClusters);
        if (needExcludeClusters.size() > maxExcludeClusters) {
            getLogger().info("[doExecute][{}] need exclude clusters too many, {}", beaconService, needExcludeClusters);
            future().setFailure(new TooManyNeedExcludeClusterException(needExcludeClusters));
            return;
        }

        Set<String> excludeClusters = new HashSet<>();
        for (String cluster: needExcludeClusters) {
            try {
                beaconService.unregisterCluster(cluster);
                excludeClusters.add(cluster);
            } catch (Throwable th) {
                getLogger().info("[doExecute][{}] unregister cluster {} fail", beaconService, cluster, th);
            }
        }

        future().setSuccess(excludeClusters);
    }

    @Override
    protected void doReset() {
    }

    @Override
    public String getName() {
        return "[UnknownClusterExcludeJob] " + beaconService;
    }
}
