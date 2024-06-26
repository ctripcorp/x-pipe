package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.message.DelayAlertRecoverySubscriber;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */

@Component
public class AlertRecoverySubscriber extends AbstractAlertEntitySubscriber implements DelayAlertRecoverySubscriber {

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

        if(alert.getAlertType().delayedSendingTime() != 0) {
            doProcessDelayAlerts(alert);
        } else {
            doAddAlert(alert);
        }
    }


    private void doAddAlert(AlertEntity alert) {
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
            doSend(holderManager, false);
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

    // 延迟告警初次加入入口
    @Override
    public void addDelayAlerts(AlertEntity alert) {
        doAddAlert(alert);
    }

    // 延迟告警处理入口(非初次)
    @Override
    public void doProcessDelayAlerts(AlertEntity alert) {
        if(unRecoveredAlerts.contains(alert)) {
            doAddAlert(alert);
        }
    }

    @VisibleForTesting
    public Set<AlertEntity> getExistingAlerts() {
        return unRecoveredAlerts;
    }

}