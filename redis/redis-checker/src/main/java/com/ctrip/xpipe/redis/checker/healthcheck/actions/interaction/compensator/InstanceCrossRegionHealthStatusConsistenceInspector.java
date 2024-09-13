package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.OutClientInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.UpDownInstances;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

@Service
public class InstanceCrossRegionHealthStatusConsistenceInspector extends AbstractLifecycle implements TopElement {
    private InstanceHealthStatusCollector collector;

    private InstanceStatusAdjuster adjuster;

    private StabilityHolder siteStability;

    private CheckerConfig config;

    private MetaCache metaCache;

    private GroupCheckerLeaderElector leaderElector;

    private DynamicDelayPeriodTask task;

    private ScheduledExecutorService scheduled;

    private static final Logger logger = LoggerFactory.getLogger(InstanceCrossRegionHealthStatusConsistenceInspector.class);

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    private static final String TYPE = "HealthCheck";

    @Autowired
    public InstanceCrossRegionHealthStatusConsistenceInspector(InstanceHealthStatusCollector instanceHealthStatusCollector,
                                                               InstanceStatusAdjuster instanceStatusAdjuster,
                                                               @Nullable GroupCheckerLeaderElector groupCheckerLeaderElector,
                                                               StabilityHolder stabilityHolder, CheckerConfig checkerConfig,
                                                               MetaCache metaCache) {
        this.collector = instanceHealthStatusCollector;
        this.adjuster = instanceStatusAdjuster;
        this.leaderElector = groupCheckerLeaderElector;
        this.siteStability = stabilityHolder;
        this.config = checkerConfig;
        this.metaCache = metaCache;
    }

    protected void inspectCurrentDc() {
        if(!siteStability.isSiteStable()) {
            logger.debug("[inspectCrossRegion][skip] unstable");
            return;
        }
        if (null == leaderElector || !leaderElector.amILeader()) {
            logger.debug("[inspectCrossRegion][skip] not leader");
            return;
        }

        logger.debug("[inspectCrossRegion] begin");
        final long timeoutMill = System.currentTimeMillis() + config.getPingDownAfterMilli() / 2;
        TransactionMonitor.DEFAULT.logTransactionSwallowException(TYPE, "compensator.inspect.crossRegion", new Task() {
            @Override
            public void go() throws Exception {
                Map<String, Set<HostPort>> interestedCurrentDc = fetchInterestedCurrentDcClusterInstances();
                if (interestedCurrentDc.isEmpty()) {
                    logger.debug("[inspectCrossRegion][skip] no interested instance");
                    return;
                }

                Pair<XPipeInstanceHealthHolder, OutClientInstanceHealthHolder> instanceHealth = collector.collect(true);
                checkTimeout(timeoutMill, "after collect");

                XPipeInstanceHealthHolder xpipeInstanceHealth = instanceHealth.getKey();
                OutClientInstanceHealthHolder outClientInstanceHealth = instanceHealth.getValue();
                UpDownInstances hostPortNeedAdjustForPingAction = findHostPortNeedAdjust(xpipeInstanceHealth, outClientInstanceHealth, interestedCurrentDc);

                checkTimeout(timeoutMill, "after compare");
                if (!hostPortNeedAdjustForPingAction.getHealthyInstances().isEmpty())
                    adjuster.adjustInstances(hostPortNeedAdjustForPingAction.getHealthyInstances(), true, true, timeoutMill);

                checkTimeout(timeoutMill, "after adjust up");
                if (!hostPortNeedAdjustForPingAction.getUnhealthyInstances().isEmpty())
                    adjuster.adjustInstances(hostPortNeedAdjustForPingAction.getUnhealthyInstances(), true, false, timeoutMill);
            }

            @Override
            public Map getData() {
                return Collections.singletonMap("timeoutMilli", timeoutMill);
            }
        });
    }

    private void checkTimeout(long timeoutAtMilli, String msg) throws TimeoutException {
        if (System.currentTimeMillis() > timeoutAtMilli) {
            logger.info("[timeout] {}", msg);
            throw new TimeoutException(msg);
        }
    }

    protected Map<String, Set<HostPort>> fetchInterestedCurrentDcClusterInstances() {
        Map<String, Set<HostPort>> interestedCurrentDcClusterInstances = new HashMap<>();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return interestedCurrentDcClusterInstances;

        for (DcMeta dcMeta: xpipeMeta.getDcs().values()) {
            if (!dcMeta.getId().equalsIgnoreCase(currentDc)) continue;

            for (ClusterMeta clusterMeta: dcMeta.getClusters().values()) {
                try {
                    if (!ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.ONE_WAY)) continue;
                    if (!metaCache.isCrossRegion(dcMeta.getId(), clusterMeta.getActiveDc())) continue;

                    Set<HostPort> interestedInstances = MapUtils.getOrCreate(interestedCurrentDcClusterInstances, clusterMeta.getId(), HashSet::new);
                    for (ShardMeta shardMeta: clusterMeta.getShards().values()) {
                        shardMeta.getRedises().forEach(redis -> interestedInstances.add(new HostPort(redis.getIp(), redis.getPort())));
                    }
                } catch (Exception e) {
                    logger.error("fetch interested currentDc cluster instances err, clusterMeta:{}", clusterMeta, e);
                }
            }
        }

        return interestedCurrentDcClusterInstances;
    }

    protected UpDownInstances findHostPortNeedAdjust(XPipeInstanceHealthHolder xpipeInstanceHealthHolder,
                                                     OutClientInstanceHealthHolder outClientInstanceHealthHolder,
                                                     Map<String, Set<HostPort>> interested) {
        int quorum = config.getQuorum();
        UpDownInstances xpipeInstances = xpipeInstanceHealthHolder.aggregate(interested, quorum);
        UpDownInstances outClientInstances = outClientInstanceHealthHolder.extractReadable(interested);

        Set<HostPort> needMarkUpInstances = xpipeInstances.getHealthyInstances();
        Set<HostPort> needMarkDownInstances = xpipeInstances.getUnhealthyInstances();
        needMarkUpInstances.retainAll(outClientInstances.getUnhealthyInstances());
        needMarkDownInstances.retainAll(outClientInstances.getHealthyInstances());
        logger.info("[InstanceCrossRegionHealthStatusConsistenceInspector] needMarkUpInstances:{}", needMarkUpInstances);
        logger.info("[InstanceCrossRegionHealthStatusConsistenceInspector] needMarkDownInstances:{}", needMarkDownInstances);
        return new UpDownInstances(needMarkUpInstances, needMarkDownInstances);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("InstanceCrossRegionHealthStatusConsistenceInspector"));
        this.task = new DynamicDelayPeriodTask("inspectCrossRegion", new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                inspectCurrentDc();
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
