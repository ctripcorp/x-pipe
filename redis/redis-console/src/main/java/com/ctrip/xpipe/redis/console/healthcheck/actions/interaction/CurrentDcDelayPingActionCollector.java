package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.console.alert.ALERT_TYPE.CRDT_INSTANCE_DOWN;
import static com.ctrip.xpipe.redis.console.alert.ALERT_TYPE.CRDT_INSTANCE_UP;

@Component
public class CurrentDcDelayPingActionCollector extends AbstractDelayPingActionCollector implements DelayPingActionCollector, BiDirectionSupport {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDelayPingActionCollector.class);

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    private AlertManager alertManager;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

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
                healthStatus.start();
                return healthStatus;
            }
        });
    }

    private void setInstanceUp(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        ClusterShardHostPort key = new ClusterShardHostPort(info.getClusterId(), info.getShardId(), info.getHostPort());
        Boolean pre = instanceHealthStatusMap.put(key, true);

        if (null != pre && !pre) {
            // not alert on first set up
            alertManager.alert(info, CRDT_INSTANCE_UP, "Instance Up");
        }
    }

    private void setInstanceDown(RedisHealthCheckInstance instance, AbstractInstanceEvent event) {
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        ClusterShardHostPort key = new ClusterShardHostPort(info.getClusterId(), info.getShardId(), info.getHostPort());
        Boolean pre = instanceHealthStatusMap.put(key, false);

        if (null == pre || pre) {
            // alert on first set down
            alertManager.alert(info, CRDT_INSTANCE_DOWN, "cause:" + event);
        }
    }

    @Override
    public boolean supportInstance(RedisHealthCheckInstance instance) {
        return currentDcId.equalsIgnoreCase(instance.getRedisInstanceInfo().getDcId());
    }

    @VisibleForTesting
    protected void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

}
