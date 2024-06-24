package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.policy.receiver.EmailReceiverModel;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */

@Component
public class AlertRecoverySubscriber extends AbstractAlertEntitySubscriber {

    private Set<AlertEntity> unRecoveredAlerts = Sets.newConcurrentHashSet();

    @PostConstruct
    public void scheduledRecoverAlertReport() {
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {

                logger.debug("[scheduledRecoverAlertReport] unRecoveredAlerts: {}", unRecoveredAlerts);
                try {
                    lock.lock();
                    if (unRecoveredAlerts.isEmpty()) {
                        return;
                    }

                    reportRecovered();
                } finally {
                    lock.unlock();
                }
            }
        }, 1, 3, TimeUnit.MINUTES);
    }

    @VisibleForTesting
    protected void reportRecovered() {
        RecoveredAlertCleaner cleaner = new RecoveredAlertCleaner();
        cleaner.future().addListener(commandFuture -> {
            if(commandFuture.isSuccess()) {
                Set<AlertEntity> recovered = commandFuture.getNow();
                new ReportRecoveredAlertTask(recovered).execute(executors);
            } else {
                logger.warn("[reportRecovered]", commandFuture.cause());
            }
        });
        cleaner.execute(executors);
    }

    @Override
    protected void doProcessAlert(AlertEntity alert) {
        if(!alert.getAlertType().reportRecovery()) {
            logger.debug("[doProcessAlert]Not interested: {}", alert);
            return;
        }

        while(!unRecoveredAlerts.add(alert)) {
            logger.debug("[doProcessAlert]add alert: {}", alert);
            unRecoveredAlerts.remove(alert);
        }
    }

    class RecoveredAlertCleaner extends AbstractCommand<Set<AlertEntity>> {

        @Override
        protected void doExecute() throws Exception {
            Set<AlertEntity> recovered = Sets.newHashSet();
            if(unRecoveredAlerts == null || unRecoveredAlerts.isEmpty()) {
                future().setSuccess(recovered);
                return;
            }
            for(AlertEntity alert : unRecoveredAlerts) {
                if(alertRecovered(alert)) {
                    recovered.add(alert);
                }
            }
            unRecoveredAlerts.removeAll(recovered);
            logger.debug("[RecoveredAlertCleaner][recovered] {}", recovered);
            logger.debug("[RecoveredAlertCleaner][un-recovered] {}", unRecoveredAlerts);
            future().setSuccess(recovered);
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return RecoveredAlertCleaner.class.getSimpleName();
        }
    }

    class ReportRecoveredAlertTask extends AbstractCommand<Void> {

        private Set<AlertEntity> alerts;

        public ReportRecoveredAlertTask(Set<AlertEntity> alerts) {
            this.alerts = alerts;
        }

        @Override
        protected void doExecute() throws Exception {
            if(alerts == null || alerts.isEmpty()) {
                return;
            }
            AlertEntityHolderManager holderManager = new DefaultAlertEntityHolderManager();
            addAlertsToAlertHolders(alerts, holderManager);
            Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> map = alertPolicyManager().queryGroupedEmailReceivers(holderManager);

            for(Map.Entry<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> mailGroup : map.entrySet()) {
                if(mailGroup.getValue() == null || mailGroup.getValue().isEmpty()) {
                    continue;
                }
                Map<ALERT_TYPE, Set<AlertEntity>> alerts = mailGroup.getValue();
                transmitAlterToCheckerLeader(false, alerts);
                if(alerts.size() == 0) {
                    continue;
                }
                AlertMessageEntity message = getMessage(mailGroup.getKey(), alerts, false);
                emailMessage(message);
                tryMetric(mailGroup.getValue(), false);
            }
            future().setSuccess();
        }



        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return ReportRecoveredAlertTask.class.getSimpleName();
        }
    }

}
