package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.HealthEventProcessor;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class CRDTDelayPingActionCollector extends AbstractDelayPingActionCollector implements DelayPingActionCollector, BiDirectionSupport {

    private static final Logger logger = LoggerFactory.getLogger(CRDTDelayPingActionCollector.class);

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    private AlertManager alertManager;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Autowired
    private List<HealthEventProcessor> healthEventProcessors;

    private Map<ClusterShardHostPort, Boolean> instanceHealthStatusMap = Maps.newConcurrentMap();

    @Override
    protected HealthStatus createOrGetHealthStatus(RedisHealthCheckInstance instance) {

        return MapUtils.getOrCreate(allHealthStatus, instance, new ObjectFactory<HealthStatus>() {
            @Override
            public HealthStatus create() {
                HealthStatus healthStatus = new HealthStatus(instance, scheduled);
                if (currentDcId.equals(instance.getCheckInfo().getDcId())) {
                    healthStatus.addObserver(new CurrentDCInstanceObserver(instance));
                } else {
                    healthStatus.addObserver(new PeerObserver(instance));
                }
                healthStatus.addObserver((args, observable) -> onInstanceHealthStateChange(args));
                healthStatus.start();
                return healthStatus;
            }
        });
    }

    private class CurrentDCInstanceObserver implements Observer {

        private final RedisHealthCheckInstance instance;

        public CurrentDCInstanceObserver(RedisHealthCheckInstance instance) {
            this.instance = instance;
        }

        @Override
        public void update(Object args, Observable observable) {
            logger.info("[currentDcInstanceHealthStatusChange]{}", args);
            if (args instanceof InstanceDown || args instanceof InstanceSick) {
                setInstanceDown((AbstractInstanceEvent) args);
            } else if (args instanceof InstanceUp) {
                setInstanceUp();
            }
        }

        private void setInstanceUp() {
            RedisInstanceInfo info = instance.getCheckInfo();
            ClusterShardHostPort key = new ClusterShardHostPort(info.getClusterId(), info.getShardId(), info.getHostPort());
            Boolean pre = instanceHealthStatusMap.put(key, true);

            if (null != pre && !pre) {
                // not alert on first set up
                alertManager.alert(info, ALERT_TYPE.CRDT_INSTANCE_UP, "Instance Up");
            }
        }

        private void setInstanceDown(AbstractInstanceEvent event) {
            RedisInstanceInfo info = instance.getCheckInfo();
            ClusterShardHostPort key = new ClusterShardHostPort(info.getClusterId(), info.getShardId(), info.getHostPort());
            Boolean pre = instanceHealthStatusMap.put(key, false);

            if (null == pre || pre) {
                // alert on first set down
                alertManager.alert(info, ALERT_TYPE.CRDT_INSTANCE_DOWN, "cause:" + event);
            }
        }
    }

    private class PeerObserver implements Observer {

        private AtomicBoolean health = new AtomicBoolean(true);

        private final String clusterId;
        private final String shardId;
        private final String targetDc;

        public PeerObserver(RedisHealthCheckInstance instance) {
            RedisInstanceInfo info = instance.getCheckInfo();

            this.clusterId = info.getClusterId();
            this.shardId = info.getShardId();
            this.targetDc = info.getDcId();
        }

        @Override
        public void update(Object args, Observable observable) {
            logger.info("[peerStateChange]{}", args);
            if ((args instanceof InstanceSick || args instanceof InstanceLongDelay) && health.compareAndSet(true, false)) {
                logger.info("[onCurrentMasterUnhealthy] cluster {}, shard {} become unhealthy for target dc {}", clusterId, shardId, targetDc);
                alertManager.alert(clusterId, shardId, null, ALERT_TYPE.CRDT_CROSS_DC_REPLICATION_DOWN, String.format("replication unhealthy from %s to %s", currentDcId, targetDc));
            } else if (args instanceof InstanceUp && health.compareAndSet(false, true)) {
                logger.info("[onCurrentMasterHealthy] cluster {}, shard {} become healthy", clusterId, shardId);
                alertManager.alert(clusterId, shardId, null, ALERT_TYPE.CRDT_CROSS_DC_REPLICATION_UP, "replication become healthy from " + currentDcId);
            }
        }
    }

    private void onInstanceHealthStateChange(Object args) {

        logger.info("[onInstanceStateChange]{}", args);

        if (healthEventProcessors == null) {
            return;
        }

        for (HealthEventProcessor processor : healthEventProcessors) {

            if (processor instanceof BiDirectionSupport) {
                executors.execute(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        processor.onEvent((AbstractInstanceEvent) args);
                    }
                });
            }
        }
    }

    @VisibleForTesting
    protected void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

}
