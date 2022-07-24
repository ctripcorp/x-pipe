package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.OutClientInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.UpDownInstances;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author lishanglin
 * date 2022/7/21
 */
@Service
public class InstanceHealthStatusConsistenceChecker extends AbstractLifecycle implements TopElement {

    private InstanceHealthStatusCollector collector;

    private InstanceStatusAdjuster adjuster;

    private CheckerConfig config;

    private MetaCache metaCache;

    private GroupCheckerLeaderElector leaderElector;

    private DynamicDelayPeriodTask task;

    private ScheduledExecutorService scheduled;

    private static final Logger logger = LoggerFactory.getLogger(InstanceHealthStatusConsistenceChecker.class);

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    public InstanceHealthStatusConsistenceChecker(InstanceHealthStatusCollector instanceHealthStatusCollector,
                                                  InstanceStatusAdjuster instanceStatusAdjuster,
                                                  @Nullable GroupCheckerLeaderElector groupCheckerLeaderElector,
                                                  CheckerConfig checkerConfig, MetaCache metaCache) {
        this.collector = instanceHealthStatusCollector;
        this.adjuster = instanceStatusAdjuster;
        this.leaderElector = groupCheckerLeaderElector;
        this.config = checkerConfig;
        this.metaCache = metaCache;
    }

    @VisibleForTesting
    protected void check() throws InterruptedException, ExecutionException, TimeoutException {
        if (null == leaderElector || !leaderElector.amILeader()) {
            logger.debug("[inspect][skip] not leader");
        }

        logger.debug("[inspect] begin");
        long timeoutMill = System.currentTimeMillis() + Math.min(config.getPingDownAfterMilli() / 2,
                config.getDownAfterCheckNums() * config.getRedisReplicationHealthCheckInterval() / 2);
        Set<HostPort> interested = fetchInterestedInstance();
        if (interested.isEmpty()) {
            logger.debug("[inspect][skip] no interested instance");
            return;
        }

        Pair<XPipeInstanceHealthHolder, OutClientInstanceHealthHolder> instanceHealth = collector.collect();
        checkTimeout(timeoutMill, "after collect");

        XPipeInstanceHealthHolder xpipeInstanceHealth = instanceHealth.getKey();
        OutClientInstanceHealthHolder outClientInstanceHealth = instanceHealth.getValue();
        UpDownInstances instanceNeedAdjust = findHostPortNeedAdjust(xpipeInstanceHealth, outClientInstanceHealth, interested);

        checkTimeout(timeoutMill, "after compare");
        if (!instanceNeedAdjust.getHealthyInstances().isEmpty())
            adjuster.adjustInstances(instanceNeedAdjust.getHealthyInstances(), true, timeoutMill);

        checkTimeout(timeoutMill, "after adjust up");
        if (!instanceNeedAdjust.getUnhealthyInstances().isEmpty())
            adjuster.adjustInstances(instanceNeedAdjust.getUnhealthyInstances(), false, timeoutMill);
    }

    private void checkTimeout(long timeoutAtMilli, String msg) {
        if (System.currentTimeMillis() > timeoutAtMilli) {
            logger.info("[timeout] {}", msg);
        }
    }

    @VisibleForTesting
    protected Set<HostPort> fetchInterestedInstance() {
        Set<HostPort> interested = new HashSet<>();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return interested;

        for (DcMeta dcMeta: xpipeMeta.getDcs().values()) {
            if (dcMeta.getId().equalsIgnoreCase(currentDc)) continue;

            for (ClusterMeta clusterMeta: dcMeta.getClusters().values()) {
                if (!ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.ONE_WAY)) continue;
                if (!clusterMeta.getActiveDc().equalsIgnoreCase(currentDc)) continue;

                for (ShardMeta shardMeta: clusterMeta.getShards().values()) {
                    shardMeta.getRedises().forEach(redis -> interested.add(new HostPort(redis.getIp(), redis.getPort())));
                }
            }
        }


        return interested;
    }

    protected UpDownInstances findHostPortNeedAdjust(XPipeInstanceHealthHolder xpipeInstanceHealthHolder,
                                                                        OutClientInstanceHealthHolder outClientInstanceHealthHolder,
                                                                        Set<HostPort> interested) {
        UpDownInstances xpipeInstances = xpipeInstanceHealthHolder.aggregate(interested, config.getQuorum());
        UpDownInstances outClientInstances = outClientInstanceHealthHolder.extractReadable(interested);

        xpipeInstances.getHealthyInstances().retainAll(outClientInstances.getUnhealthyInstances());
        xpipeInstances.getUnhealthyInstances().retainAll(outClientInstances.getHealthyInstances());
        return xpipeInstances;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("InstanceHealthStatusConsistenceInspector"));
        this.task = new DynamicDelayPeriodTask("inspect", new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                check();
            }
        }, config::getHealthMarkCompensateIntervalMill, scheduled);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.task.start();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        this.task.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        this.scheduled.shutdown();
        this.scheduled = null;
        this.task = null;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

}
