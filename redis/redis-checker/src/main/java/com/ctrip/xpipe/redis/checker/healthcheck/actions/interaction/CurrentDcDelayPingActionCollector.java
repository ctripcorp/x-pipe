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
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
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

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class CurrentDcDelayPingActionCollector extends AbstractDelayPingActionCollector implements DelayPingActionCollector, BiDirectionSupport {

    private static final Logger logger = LoggerFactory.getLogger(CurrentDcDelayPingActionCollector.class);

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
                healthStatus.addObserver(new Observer() {
                    @Override
                    public void update(Object args, Observable observable) {
                        logger.info("[currentDcInstanceHealthStatusChange]{}", args);
                        if (args instanceof InstanceDown || args instanceof InstanceSick) {
                            setInstanceDown(instance, (AbstractInstanceEvent) args);
                        } else if (args instanceof InstanceUp) {
                            setInstanceUp(instance);
                        }
                    }
                });
                healthStatus.addObserver(new Observer() {
                    @Override
                    public void update(Object args, Observable observable) {
                        onInstanceStateChange((AbstractInstanceEvent) args);
                    }
                });
                healthStatus.start();
                return healthStatus;
            }
        });
    }

    private void setInstanceUp(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        ClusterShardHostPort key = new ClusterShardHostPort(info.getClusterId(), info.getShardId(), info.getHostPort());
        Boolean pre = instanceHealthStatusMap.put(key, true);

        if (null != pre && !pre) {
            // not alert on first set up
            alertManager.alert(info, ALERT_TYPE.CRDT_INSTANCE_UP, "Instance Up");
        }
    }

    private void setInstanceDown(RedisHealthCheckInstance instance, AbstractInstanceEvent event) {
        RedisInstanceInfo info = instance.getCheckInfo();
        ClusterShardHostPort key = new ClusterShardHostPort(info.getClusterId(), info.getShardId(), info.getHostPort());
        Boolean pre = instanceHealthStatusMap.put(key, false);

        if (null == pre || pre) {
            // alert on first set down
            alertManager.alert(info, ALERT_TYPE.CRDT_INSTANCE_DOWN, "cause:" + event);
        }
    }

    private void onInstanceStateChange(Object args) {

        logger.info("[onInstanceStateChange]{}", args);
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

    @Override
    public boolean supportInstance(RedisHealthCheckInstance instance) {
        return currentDcId.equalsIgnoreCase(instance.getCheckInfo().getDcId());
    }

    @VisibleForTesting
    protected void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

}
