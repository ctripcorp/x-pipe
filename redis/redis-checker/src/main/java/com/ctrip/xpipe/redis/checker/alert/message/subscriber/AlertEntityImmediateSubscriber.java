package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */

@Component
public class AlertEntityImmediateSubscriber extends AbstractAlertEntitySubscriber {

    protected static final Logger logger = LoggerFactory.getLogger(AlertEntityImmediateSubscriber.class);

    private final static long reportInterval = 3 * 60 * 1000;

    private Set<AlertEntity> existingAlerts = Sets.newConcurrentHashSet();

    private Set<AlertEntity> sendingAlerts = Sets.newConcurrentHashSet();

    private AtomicBoolean sendTaskBegin = new AtomicBoolean();

    @PostConstruct
    public void initCleaner() {
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                try {
                    lock.lock();
                    logger.debug("[ImmediateSubscriber][before]sent alerts: {}", existingAlerts);
                    existingAlerts.removeIf(alert -> alertRecovered(alert));
                    logger.debug("[ImmediateSubscriber][after]send alerts: {}", existingAlerts);
                } finally {
                    lock.unlock();
                }

            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    protected void doProcessAlert(AlertEntity alert) {
        if(alert.getAlertType().delayedSendingTime() > 0 || alert.getAlertType().onlyTrack()) {
            return;
        }
        if(hasBeenSentOut(alert)) {
            logger.debug("[ImmediateSubscriber]Alert has been sent out once: {}", alert);
            return;
        }
        logger.debug("[ImmediateSubscriber]sendTaskBegin {}", sendTaskBegin.get());
        sendingAlerts.add(alert);
        if(sendTaskBegin.compareAndSet(false, true)) {
            logger.debug("send alert @{}, alert: {}", DateTimeUtils.currentTimeAsString(), alert);
            scheduleSendTask();
        }
    }

    private void scheduleSendTask() {
        scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                AlertEntityHolderManager holderManager = new DefaultAlertEntityHolderManager();
                try {
                    lock.lock();
                    addAlertsToAlertHolders(sendingAlerts, holderManager);
                    sendingAlerts = Sets.newConcurrentHashSet();
                } finally {
                    lock.unlock();
                }
                logger.debug("[ImmediateSubscriber][alerts] {}", holderManager);
                doSend(holderManager, true);
                sendTaskBegin.compareAndSet(true, false);
            }
        }, reportInterval, TimeUnit.MILLISECONDS);
    }

    private boolean hasBeenSentOut(AlertEntity alert) {
        try {
            lock.lock();
            boolean result = existingAlerts.contains(alert);

            // replace the old alert (update alert time), add operation wont replace the existing one
            if (result) {
                existingAlerts.remove(alert);
            }
            existingAlerts.add(alert);

            return result;
        } finally {
            lock.unlock();
        }
    }


    @VisibleForTesting
    public Set<AlertEntity> getExistingAlerts() {
        return existingAlerts;
    }

    @VisibleForTesting
    public Set<AlertEntity> getSendingAlerts() {
        return sendingAlerts;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}
