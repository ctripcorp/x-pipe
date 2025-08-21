package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHandler;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntitySubscriber;
import com.ctrip.xpipe.redis.checker.alert.event.EventBus;
import com.ctrip.xpipe.redis.checker.alert.policy.receiver.EmailReceiverModel;
import com.ctrip.xpipe.redis.checker.resource.CheckLeaderService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public abstract class AbstractAlertEntitySubscriber extends AlertEntityHandler implements AlertEntitySubscriber {

    protected final Lock lock = new ReentrantLock();

    @Autowired
    private AlertConfig alertConfig;

    @Autowired(required = false)
    private CheckLeaderService checkLeaderService;

    @Resource(name = GLOBAL_EXECUTOR)
    protected ExecutorService executors;

    @Override
    public void register(EventBus eventBus) {
        eventBus.register(this);
    }

    @Override
    public void unregister(EventBus eventBus) {
        eventBus.unregister(this);
    }

    @Override
    public void processData(AlertEntity alert) {
        getLogger().debug("process alert: {}", alert);
        doProcessAlert(alert);
    }

    protected abstract void doProcessAlert(AlertEntity alert);

    protected long recoveryMilli(AlertEntity alert) {
        return alertPolicyManager.queryRecoverMilli(alert);
    }

    protected boolean alertRecovered(AlertEntity alert) {
        long recoveryMilli = recoveryMilli(alert);
        long expectedRecoverMilli = recoveryMilli + alert.getDate().getTime();
        if(expectedRecoverMilli <= System.currentTimeMillis()) {
            getLogger().debug("[alertRecovered] alert: {}, expected: {}, now: {}", DateTimeUtils.timeAsString(alert.getDate()),
                    DateTimeUtils.timeAsString(expectedRecoverMilli), DateTimeUtils.currentTimeAsString());
            return true;
        }
        return false;
    }

    protected AlertConfig alertConfig() {
        return alertConfig;
    }

    protected void transmitAlterToCheckerLeader(boolean isAlert, Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        List<AlertEntity> sendingAlerts = new ArrayList<>();
        for(Map.Entry<ALERT_TYPE, Set<AlertEntity>> entry : alerts.entrySet()) {
            ALERT_TYPE alertType = entry.getKey();
            if(alertType.sendToCheckerLeader()) {
                sendingAlerts.addAll(entry.getValue());
            }
        }

        if(sendingAlerts.size() == 0) {
            return;
        }
        String alertType = isAlert ? "alert" : "recover";
        boolean sendStatus = checkLeaderService.sendAlertToCheckerLeader(alertType, sendingAlerts);
        if(sendStatus) {
            Iterator<Map.Entry<ALERT_TYPE, Set<AlertEntity>>> iterator = alerts.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ALERT_TYPE, Set<AlertEntity>> entry = iterator.next();
                if(entry.getKey().sendToCheckerLeader()) {
                    iterator.remove();
                }
            }
        }
    }

    protected void doSend(AlertEntityHolderManager holderManager, boolean isAlert) {
        Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> map = alertPolicyManager().queryGroupedEmailReceivers(holderManager);

        for(Map.Entry<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> mailGroup : map.entrySet()) {
            if(mailGroup.getValue() == null || mailGroup.getValue().isEmpty()) {
                continue;
            }
            getLogger().debug("[AlertEntitySubscriber] Mail out: {}", mailGroup.getValue());
            Map<ALERT_TYPE, Set<AlertEntity>> alerts = mailGroup.getValue();
            transmitAlterToCheckerLeader(isAlert, alerts);
            if(alerts.size() == 0) {
                continue;
            }
            AlertMessageEntity message = getMessage(mailGroup.getKey(), alerts, isAlert);
            emailMessage(message);
            tryMetric(alerts, isAlert);
        }
    }
}