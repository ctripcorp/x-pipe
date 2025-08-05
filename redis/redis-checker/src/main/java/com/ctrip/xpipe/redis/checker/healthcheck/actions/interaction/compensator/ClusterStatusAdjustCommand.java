package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.migration.OuterClientService.HostPortDcStatus;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.ClusterActiveDcKey;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClusterStatusAdjustCommand extends AbstractCommand<Void> {

    private ClusterActiveDcKey clusterActiveDcKey;

    private List<ClusterShardHostPort> instances;

    private long deadlineTimeMilli;

    private MetaCache metaCache;

    private StabilityHolder siteStability;

    private CheckerConfig config;

    private AggregatorPullService aggregatorPullService;

    private static final Logger logger = LoggerFactory.getLogger(ClusterStatusAdjustCommand.class);

    public ClusterStatusAdjustCommand(ClusterActiveDcKey clusterActiveDc, List<ClusterShardHostPort> instances, long deadlineTimeMilli,
                                      StabilityHolder stabilityHolder, CheckerConfig config, MetaCache metaCache,
                                      AggregatorPullService aggregatorPullService) {
        this.clusterActiveDcKey = clusterActiveDc;
        this.instances = instances;
        this.siteStability = stabilityHolder;
        this.config = config;
        this.deadlineTimeMilli = deadlineTimeMilli;
        this.metaCache = metaCache;
        this.aggregatorPullService = aggregatorPullService;
    }

    @Override
    protected void doExecute() throws Throwable {
        if (checkTimeout()) return;
        if (!siteStability.isSiteStable()) {
            logger.info("[compensate][skip][unstable]{}, {}", clusterActiveDcKey, instances);
            future().setSuccess();
            return;
        }

        Set<HostPort> backupDcInstances = backupDcInstances();
        if (backupDcInstances.isEmpty()) {
            logger.info("[compensate][skip][active dc instance]{}, {}", clusterActiveDcKey, instances);
            future().setFailure(new IllegalArgumentException("instance not in backup dc"));
            return;
        }

        Set<HostPortDcStatus> needAdjustInstances = aggregatorPullService.getNeedAdjustInstances(clusterActiveDcKey.getCluster(), backupDcInstances);
        if (null == needAdjustInstances || needAdjustInstances.isEmpty()) {
            logger.info("[compensate][skip][empty needAdjustInstances]{}, {}", clusterActiveDcKey, instances);
            future().setSuccess();
            return;
        }

        if (checkTimeout()) return;
        logger.info("[compensate]{},{}", clusterActiveDcKey, needAdjustInstances);
        long noModifySeconds = TimeUnit.MILLISECONDS.toSeconds(config.getHealthMarkCompensateIntervalMill());
        aggregatorPullService.doMarkInstancesIfNoModifyFor(clusterActiveDcKey.getCluster(), clusterActiveDcKey.getActiveDc(), needAdjustInstances, noModifySeconds);

        future().setSuccess();
    }

    private boolean checkTimeout() {
        if (System.currentTimeMillis() > deadlineTimeMilli) {
            logger.info("[compensate][skip] timeout {}, cluster {}, instance {}", deadlineTimeMilli, clusterActiveDcKey, instances);
            future().setFailure(new TimeoutException(clusterActiveDcKey.toString()));
            return true;
        }

        return false;
    }

    private Set<HostPort> backupDcInstances() {
        Set<HostPort> backupDcInstances = new HashSet<>();
        for (ClusterShardHostPort instance : instances) {
            if (metaCache.inBackupDc(instance.getHostPort())) {
                backupDcInstances.add(instance.getHostPort());
            }
        }
        return backupDcInstances;
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return "ClusterStatusAdjustCommand:" + clusterActiveDcKey;
    }
}
