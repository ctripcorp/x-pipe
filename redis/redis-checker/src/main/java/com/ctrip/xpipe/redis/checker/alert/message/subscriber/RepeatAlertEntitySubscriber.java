package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolder;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.policy.receiver.EmailReceiverModel;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */
@Component
public class RepeatAlertEntitySubscriber extends AbstractAlertEntitySubscriber {

    private AlertEntityHolderManager holderManager = new DefaultAlertEntityHolderManager();

    private List<ALERT_TYPE> ignoredAlertType = Lists.newArrayList(
            ALERT_TYPE.ALERT_SYSTEM_OFF, ALERT_TYPE.SENTINEL_AUTO_PROCESS_OFF);

    @PostConstruct
    public void scheduledTask() {
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                logger.debug("[scheduledTask]Stored alerts: {}", holderManager);
                scheduledReport();

            }
        }, 1, alertConfig().getAlertSystemSuspendMinute(), TimeUnit.MINUTES);
    }

    @VisibleForTesting
    protected void scheduledReport() {
        AlertEntityHolderManager prevHolderManager;
        synchronized (this) {
            prevHolderManager = refresh();
        }
        logger.debug("[scheduledReport]Keep receiving alerts: {}", holderManager.allAlertsToSend());
        if(prevHolderManager == null || !prevHolderManager.hasAlertsToSend()) {
            return;
        }

        Command<AlertEntityHolderManager> clearTask = new ScheduledCleanupExpiredAlertTask(prevHolderManager);

        clearTask.future().addListener(new CommandFutureListener<AlertEntityHolderManager>() {
            @Override
            public void operationComplete(CommandFuture<AlertEntityHolderManager> commandFuture) {
                if(commandFuture.isSuccess()) {
                    new ScheduledSendRepeatAlertTask(commandFuture.getNow()).execute(executors);
                } else {
                    logger.error("[ScheduledCleanupExpiredAlertTask]", commandFuture.cause());
                }
            }
        });

        clearTask.execute(executors);
    }

    @Override
    protected void doProcessAlert(AlertEntity alert) {
        if(ignoreAlert(alert)) {
            return;
        }
        synchronized (this) {
            holderManager.holdAlert(alert);
        }
    }

    private AlertEntityHolderManager refresh() {
        AlertEntityHolderManager prevHolderManager = holderManager;
        holderManager = new DefaultAlertEntityHolderManager();
        return prevHolderManager;
    }

    private boolean ignoreAlert(AlertEntity alert) {
        return ignoredAlertType.contains(alert.getAlertType());
    }

    class ScheduledSendRepeatAlertTask extends AbstractCommand<Void> {

        private AlertEntityHolderManager alerts;

        public ScheduledSendRepeatAlertTask(AlertEntityHolderManager alerts) {
            this.alerts = alerts;
        }

        @Override
        protected void doExecute() throws Exception {
            Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> map = alertPolicyManager().queryGroupedEmailReceivers(alerts);

            for(Map.Entry<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> mailGroup : map.entrySet()) {
                if(mailGroup.getValue() == null || mailGroup.getValue().isEmpty()) {
                    continue;
                }
                logger.debug("[ScheduledSendRepeatAlertTask] Mail out: {}", mailGroup.getValue());
                Map<ALERT_TYPE, Set<AlertEntity>> alerts = mailGroup.getValue();
                transmitAlterToCheckerLeader(true, alerts);
                if(alerts.size() == 0) {
                    continue;
                }
                AlertMessageEntity message = getMessage(mailGroup.getKey(), alerts, true);
                emailMessage(message);
                tryMetric(mailGroup.getValue(), true);
            }
            future().setSuccess();
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return ScheduledSendRepeatAlertTask.class.getSimpleName();
        }

    }

    class ScheduledCleanupExpiredAlertTask extends AbstractCommand<AlertEntityHolderManager> {

        private AlertEntityHolderManager alertEntityHolderManager;

        public ScheduledCleanupExpiredAlertTask(AlertEntityHolderManager alertEntityHolderManager) {
            this.alertEntityHolderManager = alertEntityHolderManager;
        }

        @Override
        protected void doExecute() throws Exception {
            AlertEntityHolderManager result = new DefaultAlertEntityHolderManager();
            try {
                for (AlertEntityHolder holder : alertEntityHolderManager.allAlertsToSend()) {
                    if (!holder.hasAlerts()) {
                        continue;
                    }
                    holder.removeIf(RepeatAlertEntitySubscriber.this::alertRecovered);
                    if (holder.hasAlerts()) {
                        result.bulkInsert(holder.allAlerts());
                    }
                }
                future().setSuccess(result);
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return ScheduledCleanupExpiredAlertTask.class.getSimpleName();
        }
    }
}
