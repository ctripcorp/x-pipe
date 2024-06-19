package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.policy.receiver.EmailReceiverModel;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
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
                    logger.debug("[initCleaner][before]sent alerts: {}", existingAlerts);
                    existingAlerts.removeIf(alert -> alertRecovered(alert));
                    logger.debug("[initCleaner][after]send alerts: {}", existingAlerts);
                } finally {
                    lock.unlock();
                }

            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    protected void doProcessAlert(AlertEntity alert) {
        if(hasBeenSentOut(alert)) {
            logger.debug("[doProcessAlert]Alert has been sent out once: {}", alert);
            return;
        }
        logger.debug("[sendTaskBegin] {}", sendTaskBegin.get());
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
                logger.debug("[scheduleSendTask][alerts] {}", holderManager);
                Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> map = alertPolicyManager().queryGroupedEmailReceivers(holderManager);

                for(Map.Entry<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> mailGroup : map.entrySet()) {
                    if(mailGroup.getValue() == null || mailGroup.getValue().isEmpty()) {
                        continue;
                    }
                    Map<ALERT_TYPE, Set<AlertEntity>> alerts = mailGroup.getValue();
                    transmitAlterToCheckerLeader(true, alerts);
                    if(alerts.size() == 0) {
                        continue;
                    }
                    AlertMessageEntity message = getMessage(mailGroup.getKey(), alerts, true);
                    emailMessage(message);
                    tryMetric(alerts, true);
                }
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
}
