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
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
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
import com.ctrip.xpipe.utils.VisibleForTesting;
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

/**
 * @author lishanglin
 * date 2022/7/21
 */
@Service
public class InstanceHealthStatusConsistenceInspector extends AbstractLifecycle implements TopElement {

    private InstanceHealthStatusCollector collector;

    private InstanceStatusAdjuster adjuster;

    private StabilityHolder siteStability;

    private CheckerConfig config;

    private MetaCache metaCache;

    private GroupCheckerLeaderElector leaderElector;

    private DynamicDelayPeriodTask task;

    private ScheduledExecutorService scheduled;

    private DefaultDelayPingActionCollector defaultDelayPingActionCollector;

    private HealthCheckInstanceManager healthCheckInstanceManager;

    private static final Logger logger = LoggerFactory.getLogger(InstanceHealthStatusConsistenceInspector.class);

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    private static final String TYPE = "HealthCheck";

    @Autowired
    public InstanceHealthStatusConsistenceInspector(InstanceHealthStatusCollector instanceHealthStatusCollector,
                                                    InstanceStatusAdjuster instanceStatusAdjuster,
                                                    @Nullable GroupCheckerLeaderElector groupCheckerLeaderElector,
                                                    StabilityHolder stabilityHolder, CheckerConfig checkerConfig,
                                                    MetaCache metaCache, DefaultDelayPingActionCollector delayPingActionCollector,
                                                    HealthCheckInstanceManager healthCheckInstanceManager) {
        this.collector = instanceHealthStatusCollector;
        this.adjuster = instanceStatusAdjuster;
        this.leaderElector = groupCheckerLeaderElector;
        this.siteStability = stabilityHolder;
        this.config = checkerConfig;
        this.metaCache = metaCache;
        this.defaultDelayPingActionCollector = delayPingActionCollector;
        this.healthCheckInstanceManager = healthCheckInstanceManager;
    }

    @VisibleForTesting
    protected void inspect() {
        if(!siteStability.isSiteStable()) {
            logger.debug("[inspect][skip] unstable");
            return;
        }
        if (null == leaderElector || !leaderElector.amILeader()) {
            logger.debug("[inspect][skip] not leader");
            return;
        }

        logger.debug("[inspect] begin");
        final long timeoutMill = System.currentTimeMillis() + Math.min(config.getPingDownAfterMilli() / 2,
                config.getDownAfterCheckNums() * config.getRedisReplicationHealthCheckInterval() / 2);
        TransactionMonitor.DEFAULT.logTransactionSwallowException(TYPE, "compensator.inspect", new Task() {
            @Override
            public void go() throws Exception {
                Map<String, Set<HostPort>> interested = fetchInterestedClusterInstances();
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

    // return instance with clusterName so that we can check if out-client-service instance in the same cluster
    @VisibleForTesting
    protected Map<String, Set<HostPort>> fetchInterestedClusterInstances() {
        Map<String, Set<HostPort>> interestedClusterInstances = new HashMap<>();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return interestedClusterInstances;

        for (DcMeta dcMeta: xpipeMeta.getDcs().values()) {
            if (dcMeta.getId().equalsIgnoreCase(currentDc)) continue;

            for (ClusterMeta clusterMeta: dcMeta.getClusters().values()) {
                if (!ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.ONE_WAY)) continue;
                if (!clusterMeta.getActiveDc().equalsIgnoreCase(currentDc)) continue;

                Set<HostPort> interestedInstances = MapUtils.getOrCreate(interestedClusterInstances, clusterMeta.getId(), HashSet::new);
                for (ShardMeta shardMeta: clusterMeta.getShards().values()) {
                    shardMeta.getRedises().forEach(redis -> interestedInstances.add(new HostPort(redis.getIp(), redis.getPort())));
                }
            }
        }

        return interestedClusterInstances;
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
        needMarkDownInstances = filterMasterHealthyInstances(xpipeInstanceHealthHolder, needMarkDownInstances, quorum);
        needMarkDownInstances = filterMarkDowUnsupportedInstances(needMarkDownInstances);
        return new UpDownInstances(needMarkUpInstances, needMarkDownInstances);
    }

    protected Set<HostPort> filterMasterHealthyInstances(XPipeInstanceHealthHolder xpipeInstanceHealthHolder,
                                                         Set<HostPort> instances, int quorum) {
        Map<Pair<String, String>, Boolean> masterHealthStatusMap = new HashMap<>();
        Set<HostPort> masterHealthyInstances = new HashSet<>();
        for (HostPort instance: instances) {
            Pair<String, String> clusterShard = metaCache.findClusterShard(instance);
            if (null == clusterShard) continue;

            if (!masterHealthStatusMap.containsKey(clusterShard)) {
                try {
                    HostPort master = metaCache.findMaster(clusterShard.getKey(), clusterShard.getValue());
                    Boolean healthy = xpipeInstanceHealthHolder.aggregate(master, quorum);
                    masterHealthStatusMap.put(clusterShard, healthy);
                } catch (MasterNotFoundException e) {
                    masterHealthStatusMap.put(clusterShard, null);
                }
            }

            if (Boolean.TRUE.equals(masterHealthStatusMap.get(clusterShard))) {
                masterHealthyInstances.add(instance);
            }
        }


        return masterHealthyInstances;
    }

    protected Set<HostPort> filterMarkDowUnsupportedInstances(Set<HostPort> instances) {
        Set<HostPort> wontMarkdownInstances = new HashSet<>();
        for (HostPort instance : instances) {
            HEALTH_STATE healthState = defaultDelayPingActionCollector.getState(instance);
            if (healthState.equals(HEALTH_STATE.SICK)) {
                if (!shouldMarkdownDcClusterSickInstances(healthCheckInstanceManager.findRedisHealthCheckInstance(instance)))
                    wontMarkdownInstances.add(instance);
            }
        }
        instances.removeAll(wontMarkdownInstances);
        return instances;
    }

    boolean shouldMarkdownDcClusterSickInstances(RedisHealthCheckInstance healthCheckInstance) {
        RedisInstanceInfo info = healthCheckInstance.getCheckInfo();
        if (info.isCrossRegion()) {
            logger.info("[markdown][{} is cross region, do not call client service ]", info.getHostPort());
            return false;
        }
        if (healthCheckInstance.getHealthCheckConfig().getDelayConfig(info.getClusterId(), currentDc, info.getDcId()).getClusterLevelHealthyDelayMilli() < 0) {
            logger.info("[markdown][cluster {} dcs {}->{} distance is -1, do not call client service ]", info.getClusterId(), currentDc, info.getDcId());
            return false;
        }
        return true;
    }


    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("InstanceHealthStatusConsistenceInspector"));
        this.task = new DynamicDelayPeriodTask("inspect", new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                inspect();
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
