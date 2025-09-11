package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
public class AlertEntityOnlyTrackSubscriber extends AbstractAlertEntitySubscriber {

    protected static final Logger logger = LoggerFactory.getLogger(AlertEntityOnlyTrackSubscriber.class);

    private final static String METRIC_TYPE = "normal_alert";

    private final static long reportInterval = 1 * 60 * 1000;

    private Set<AlertEntity> sendingAlerts = Sets.newConcurrentHashSet();


    @PostConstruct
    public void postConstruct() {
        logger.info("[TrackSubscriber], start to track");
        scheduleSendTask();
    }

    @Override
    protected void doProcessAlert(AlertEntity alert) {
        if(!alert.getAlertType().onlyTrack()) {
            return;
        }
        sendingAlerts.add(alert);
    }

    private void scheduleSendTask() {
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                Set<AlertEntity> alerts = sendingAlerts;
                sendingAlerts = Sets.newConcurrentHashSet();
                logger.debug("[TrackSubscriber] {}", alerts.size());
                for(AlertEntity alert : alerts) {
                    tryMetric(alert, METRIC_TYPE);
                }
            }
        }, reportInterval, reportInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}
