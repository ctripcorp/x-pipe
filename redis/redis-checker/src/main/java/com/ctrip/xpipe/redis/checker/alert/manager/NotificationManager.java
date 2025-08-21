package com.ctrip.xpipe.redis.checker.alert.manager;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEventBus;
import com.ctrip.xpipe.redis.checker.alert.event.Subscriber;
import com.ctrip.xpipe.utils.VisibleForTesting;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executor;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class NotificationManager {

    private static final Logger logger = LoggerFactory.getLogger(NotificationManager.class.getSimpleName());

    private AlertEventBus eventBus;

    @Autowired
    private List<Subscriber<AlertEntity>> subscribers;

    @Resource(name = GLOBAL_EXECUTOR)
    private Executor executors;

    @PostConstruct
    public void start() {
        logger.info("Alert Notification Manager started");

        eventBus = new AlertEventBus(executors);

        subscribers.forEach(subscriber -> subscriber.register(eventBus));
    }


    public void addAlert(String dc, String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message) {
        AlertEntity alert = new AlertEntity(hostPort, dc, cluster, shard, message, type);
        logger.debug("[addAlert] Add Alert Entity: {}", alert);

        eventBus.post(alert);
    }

    @VisibleForTesting
    protected List<Subscriber<AlertEntity>> subscribers() {
        return subscribers;
    }
}
