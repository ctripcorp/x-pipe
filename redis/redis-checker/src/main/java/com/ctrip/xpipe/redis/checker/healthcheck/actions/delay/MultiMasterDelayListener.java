package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class MultiMasterDelayListener implements DelayActionListener, BiDirectionSupport {

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    private static final Logger logger = LoggerFactory.getLogger(MultiMasterDelayListener.class);

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CheckerConfig consoleConfig;

    @Autowired
    private AlertManager alertManager;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private Map<DcClusterShard, CrossMasterHealthStatus> multiMasterHealthStatus = Maps.newConcurrentMap();

    private ScheduledFuture<?> future;

    @PostConstruct
    public void postConstruct() {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                checkAllHealthStatus();
            }
        }, 0, consoleConfig.getRedisReplicationHealthCheckInterval(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void preDestroy() {
        if(future != null) {
            future.cancel(true);
        }
    }

    @Override
    public void onAction(DelayActionContext delayActionContext) {
        // only care about delay between dc master
        RedisHealthCheckInstance instance = delayActionContext.instance();
        RedisInstanceInfo info = instance.getCheckInfo();
        if (currentDcId.equalsIgnoreCase(delayActionContext.instance().getCheckInfo().getDcId())) return;

        String targetDcId = info.getDcId();
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();
        Long delayNano = delayActionContext.getResult();
        long delayMilli = TimeUnit.NANOSECONDS.toMillis(delayNano);

        CrossMasterHealthStatus crossMasterLastHealthDelays = getOrCreateCrossMasterHealthStatus(clusterId, shardId);
        crossMasterLastHealthDelays.updateTargetMasterDelay(targetDcId, delayMilli, instance);
    }

    @Override
    public void stopWatch(HealthCheckAction action) {
        //do nothing
    }

    private CrossMasterHealthStatus getOrCreateCrossMasterHealthStatus(String clusterId, String shardId) {
        return MapUtils.getOrCreate(multiMasterHealthStatus, new DcClusterShard(currentDcId, clusterId, shardId), () -> new CrossMasterHealthStatus(clusterId, shardId));
    }

    @VisibleForTesting
    protected void checkAllHealthStatus() {
        Set<DcClusterShard> toDeleteClusterShard = new HashSet<>();

        for (DcClusterShard dcClusterShard : multiMasterHealthStatus.keySet()) {
            if (!isDcClusterShardExist(dcClusterShard.getDcId(), dcClusterShard.getClusterId(), dcClusterShard.getShardId())) {
                toDeleteClusterShard.add(dcClusterShard);
                continue;
            }

            multiMasterHealthStatus.get(dcClusterShard).refreshCurrentMasterHealthStatus();
        }

        toDeleteClusterShard.forEach(dcClusterShard -> {
            logger.debug("[checkAllHealthStatus] remove not exist cluster shard {}", dcClusterShard);
            multiMasterHealthStatus.remove(dcClusterShard);
        });
    }

    protected void onCurrentMasterHealthy(String clusterId, String shardId) {
        logger.info("[onCurrentMasterHealthy] cluster {}, shard {} become healthy", clusterId, shardId);
        alertManager.alert(clusterId, shardId, null, ALERT_TYPE.CRDT_CROSS_DC_REPLICATION_UP, "replication become healthy from " + currentDcId);
    }

    protected void onCurrentMasterUnhealthy(String clusterId, String shardId, Set<String> unhealthyDcIds) {
        logger.info("[onCurrentMasterUnhealthy] cluster {}, shard {} become unhealthy for target dcs {}", clusterId, shardId, unhealthyDcIds);
        alertManager.alert(clusterId, shardId, null, ALERT_TYPE.CRDT_CROSS_DC_REPLICATION_DOWN, String.format("replication unhealthy from %s to %s", currentDcId, unhealthyDcIds.toString()));
    }

    private boolean isDcClusterShardExist(String dcId, String clusterId, String shardId) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        return null != xpipeMeta
                && null != xpipeMeta.getDcs().get(dcId)
                && null != xpipeMeta.getDcs().get(dcId).getClusters().get(clusterId)
                && null != xpipeMeta.getDcs().get(dcId).getClusters().get(clusterId).getShards().get(shardId);
    }

    public class CrossMasterHealthStatus {

        private String clusterId, shardId;

        private Map<String, Long> remoteMasterLastHealthDelayTimes;

        private Map<String, RedisHealthCheckInstance> remoteMasterInstances;

        private AtomicBoolean health;

        public CrossMasterHealthStatus(String clusterId, String shardId) {
            this.clusterId = clusterId;
            this.shardId = shardId;
            this.remoteMasterLastHealthDelayTimes = Maps.newConcurrentMap();
            this.remoteMasterInstances = Maps.newConcurrentMap();
            this.health = new AtomicBoolean(true);
        }

        public void updateTargetMasterDelay(String targetDcId, long delayMilli, RedisHealthCheckInstance instance) {
            logger.debug("[updateTargetMasterDelay] cluster {}, shard {} sync to dc {} delay {}", clusterId, shardId, targetDcId, delayMilli);
            remoteMasterInstances.put(targetDcId, instance);

            if (!remoteMasterLastHealthDelayTimes.containsKey(targetDcId)) {
                remoteMasterLastHealthDelayTimes.put(targetDcId, System.currentTimeMillis());
            } else if (delayMilli >= 0 && delayMilli < instance.getHealthCheckConfig().getHealthyDelayMilli()) {
                remoteMasterLastHealthDelayTimes.put(targetDcId, System.currentTimeMillis());
                if (!getHealthStatus()) {
                    refreshCurrentMasterHealthStatus();
                }
            }
        }

        public void refreshCurrentMasterHealthStatus() {
            long currentTime = System.currentTimeMillis();
            Set<String> toDeleteTargetDcIds = new HashSet<>();
            Set<String> unhealthyTargetDcIds = new HashSet<>();

            for(String targetDcId : remoteMasterLastHealthDelayTimes.keySet()) {
                if (!isDcClusterShardExist(targetDcId, clusterId, shardId)) {
                    toDeleteTargetDcIds.add(targetDcId);
                    continue;
                }

                Long lastHealthDelay = remoteMasterLastHealthDelayTimes.get(targetDcId);
                if (null != lastHealthDelay
                        && currentTime - lastHealthDelay > remoteMasterInstances.get(targetDcId).getHealthCheckConfig().delayDownAfterMilli()) {
                    unhealthyTargetDcIds.add(targetDcId);
                }
            }

            logger.debug("[refreshCurrentMasterHealthStatus] cluster {}, shard {} remove not exist targetDcId {}", clusterId, shardId, toDeleteTargetDcIds);
            toDeleteTargetDcIds.forEach(remoteMasterLastHealthDelayTimes::remove);
            toDeleteTargetDcIds.forEach(remoteMasterInstances::remove);
            updateCurrentMasterHealthStatus(clusterId, shardId, unhealthyTargetDcIds);
        }

        public boolean getHealthStatus() {
            return health.get();
        }

        private void updateCurrentMasterHealthStatus(String clusterId, String shardId, Set<String> unhealthyTargetDcIds) {
            boolean healthCheckResult = unhealthyTargetDcIds.isEmpty();

            if (healthCheckResult && health.compareAndSet(false, true)) {
                onCurrentMasterHealthy(clusterId, shardId);
            } else if (!healthCheckResult && health.compareAndSet(true, false)) {
                onCurrentMasterUnhealthy(clusterId, shardId, unhealthyTargetDcIds);
            }
        }
    }

}
