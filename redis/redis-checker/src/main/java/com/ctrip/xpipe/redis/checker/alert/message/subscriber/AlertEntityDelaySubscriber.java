package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.message.DelayAlertRecoverySubscriber;
import com.ctrip.xpipe.redis.checker.alert.message.forward.ForwardAlertService;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author qifanwang
 * <p>
 * July 1, 2024
 */

@Component
public class AlertEntityDelaySubscriber extends AbstractAlertEntitySubscriber {

    protected static final Logger logger = LoggerFactory.getLogger(AlertEntityDelaySubscriber.class);

    private final static long reportInterval = 3 * 60 * 1000;

    // value is the alert time of first appearance and has been sent
    private Map<AlertEntity, Pair<Long, Boolean>> existingAlerts = Maps.newConcurrentMap();

    private Set<AlertEntity> sendingAlerts = Sets.newConcurrentHashSet();

    private AtomicBoolean sendTaskBegin = new AtomicBoolean();

    @Autowired
    private DelayAlertRecoverySubscriber delayAlertRecoverySubscriber;

    @Autowired
    private RepeatAlertEntitySubscriber repeatAlertEntitySubscriber;

    @PostConstruct
    public void initCleaner() {
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                try {
                    lock.lock();
                    getLogger().debug("[initCleaner][before]sent alerts: {}", existingAlerts);
                    existingAlerts.entrySet().removeIf(alert -> alertRecovered(alert.getKey()));
                    getLogger().debug("[initCleaner][after]send alerts: {}", existingAlerts);
                } finally {
                    lock.unlock();
                }

            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    protected void doProcessAlert(AlertEntity alert) {
        if(alert.getAlertType().delayedSendingTime() <= 0 || alert.getAlertType().onlyTrack()) {
            return;
        }
        addToExistingSet(alert);
        if(!needSend(alert)) {
            getLogger().debug("[DelaySubscriber]Alert should not send: {}", alert);
            return;
        }
        getLogger().debug("[DelaySubscriber] sendTaskBegin {}, {}", sendTaskBegin.get(), alert);
        addToSendingSet(alert);
        if(sendTaskBegin.compareAndSet(false, true)) {
            getLogger().debug("send alert @{}, alert: {}", DateTimeUtils.currentTimeAsString(), alert);
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
                getLogger().debug("[DelaySubscriber][alerts] {}", holderManager);
                doSend(holderManager, true);
                sendTaskBegin.compareAndSet(true, false);
            }
        }, reportInterval, TimeUnit.MILLISECONDS);
    }

    private boolean needSend(AlertEntity alert) {
        Pair<Long, Boolean> alertInfo = existingAlerts.get(alert);

        long fistTime = alertInfo.getKey();
        if(alert.getDate().getTime() - fistTime < alert.getAlertType().delayedSendingTime()) {
            return false;
        }

        return !alertInfo.getValue();
    }

    private void addToExistingSet(AlertEntity alert) {
        try {
            lock.lock();
            Pair<Long, Boolean> info = existingAlerts.get(alert);

            if (info != null) {
                existingAlerts.remove(alert);
            } else {
                // first appear
                info = new Pair<>(alert.getDate().getTime(), false);
            }
            existingAlerts.put(alert, info);
        } finally {
            lock.unlock();
        }
    }

    private void addToSendingSet(AlertEntity alert) {
        try {
            lock.lock();
            sendingAlerts.add(alert);
            Pair<Long, Boolean> info = existingAlerts.get(alert);
            info.setValue(true);
            existingAlerts.put(alert, info);
        } finally {
            lock.unlock();
        }
        if(alert.getAlertType().delayedSendingTime() != 0) {
            //  延迟发布的要推送主动给AlertRecoverySubscriber，这边主动触发
            delayAlertRecoverySubscriber.addDelayAlerts(alert);
            repeatAlertEntitySubscriber.addDelayAlert(alert);
        }
    }

    @VisibleForTesting
    public Set<AlertEntity> getExistingAlerts() {
        return existingAlerts.keySet();
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
