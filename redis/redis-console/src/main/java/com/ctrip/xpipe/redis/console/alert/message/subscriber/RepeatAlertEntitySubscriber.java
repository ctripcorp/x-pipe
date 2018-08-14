package com.ctrip.xpipe.redis.console.alert.message.subscriber;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.policy.receiver.EmailReceiverModel;
import com.ctrip.xpipe.redis.console.alert.sender.AbstractSender;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

    private Map<ALERT_TYPE, Set<AlertEntity>> repeatAlerts = Maps.newConcurrentMap();

    private List<ALERT_TYPE> ignoredAlertType = Lists.newArrayList(
            ALERT_TYPE.ALERT_SYSTEM_OFF, ALERT_TYPE.SENTINEL_AUTO_PROCESS_OFF);

    @PostConstruct
    public void scheduledTask() {
        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                logger.debug("[scheduledTask]Stored alerts: {}", repeatAlerts);
                scheduledReport();

            }
        }, 1, consoleConfig().getAlertSystemSuspendMinute(), TimeUnit.MINUTES);
    }

    @VisibleForTesting
    protected void scheduledReport() {
        Map<ALERT_TYPE, Set<AlertEntity>> alerts;
        synchronized (this) {
            alerts = refresh();
        }
        logger.debug("[scheduledReport]Keep receiving alerts: {}", alerts);
        if(alerts == null || alerts.isEmpty()) {
            return;
        }

        SequenceCommandChain chain = new SequenceCommandChain();

        chain.add(new ScheduledCleanupExpiredAlertTask(alerts));
        chain.add(new ScheduledSendRepeatAlertTask(alerts));

        chain.execute(executors);
    }

    @Override
    protected void doProcessAlert(AlertEntity alert) {
        if(ignoreAlert(alert)) {
            return;
        }
        synchronized (this) {
            repeatAlerts.putIfAbsent(alert.getAlertType(), Sets.newConcurrentHashSet());
        }
        Set<AlertEntity> alerts = repeatAlerts.get(alert.getAlertType());
        for(int i = 0; i < 3 && !alerts.add(alert); i++) {
            alerts.remove(alert);
        }

    }

    private Map<ALERT_TYPE, Set<AlertEntity>> refresh() {
        Map<ALERT_TYPE, Set<AlertEntity>> alerts = repeatAlerts;
        repeatAlerts = Maps.newConcurrentMap();
        return alerts;
    }

    private AlertMessageEntity getMessage(EmailReceiverModel receivers, Map<ALERT_TYPE, Set<AlertEntity>> alerts) {

        Pair<String, String> titleAndContent = decoratorManager().generateTitleAndContent(alerts);
        AlertMessageEntity message = new AlertMessageEntity(titleAndContent.getKey(), titleAndContent.getValue(),
                receivers.getRecipients());
        message.addParam(AbstractSender.CC_ER, receivers.getCcers());

        return message;
    }

    private boolean ignoreAlert(AlertEntity alert) {
        return ignoredAlertType.contains(alert.getAlertType());
    }

    class ScheduledSendRepeatAlertTask extends AbstractCommand<Void> {

        private Map<ALERT_TYPE, Set<AlertEntity>> alerts;

        public ScheduledSendRepeatAlertTask(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
            this.alerts = alerts;
        }

        @Override
        protected void doExecute() throws Exception {
            Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> map = alertPolicyManager().queryGroupedEmailReceivers(alerts);

            for(Map.Entry<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> mailGroup : map.entrySet()) {
                if(mailGroup.getValue() == null || mailGroup.getValue().isEmpty()) {
                    continue;
                }
                logger.info("[ScheduledSendRepeatAlertTask] Mail out: {}", mailGroup.getValue());
                AlertMessageEntity message = getMessage(mailGroup.getKey(), mailGroup.getValue());
                emailMessage(message);
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

    class ScheduledCleanupExpiredAlertTask extends AbstractCommand<Void> {

        private Map<ALERT_TYPE, Set<AlertEntity>> alerts;

        public ScheduledCleanupExpiredAlertTask(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
            this.alerts = alerts;
        }

        @Override
        protected void doExecute() throws Exception {
            for(ALERT_TYPE type : alerts.keySet()) {
                Set<AlertEntity> alertEntitySet = alerts.remove(type);
                if(alertEntitySet == null) {
                    continue;
                }
                alertEntitySet.removeIf(alert -> alertRecovered(alert));
                if(!alertEntitySet.isEmpty()) {
                    alerts.put(type, alertEntitySet);
                }
            }
            future().setSuccess();
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
