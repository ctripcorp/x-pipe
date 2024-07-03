package com.ctrip.xpipe.redis.checker.alert.message;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertChannel;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.checker.alert.manager.DecoratorManager;
import com.ctrip.xpipe.redis.checker.alert.manager.SenderManager;
import com.ctrip.xpipe.redis.checker.alert.policy.receiver.EmailReceiverModel;
import com.ctrip.xpipe.redis.checker.alert.sender.AbstractSender;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

public class AlertEntityHandler {

    protected static final Logger logger = LoggerFactory.getLogger(AlertEntityHandler.class);

    protected final Lock lock = new ReentrantLock();

    @Resource(name = SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService scheduled;

    @Autowired
    protected SenderManager senderManager;

    @Autowired
    protected DecoratorManager decoratorManager;

    private MetricProxy metricProxy = MetricProxy.DEFAULT;

    @Autowired
    protected AlertPolicyManager alertPolicyManager;

    protected AlertPolicyManager alertPolicyManager() {
        return alertPolicyManager;
    }

    protected DecoratorManager decoratorManager() {
        return decoratorManager;
    }

    protected AlertMessageEntity getMessage(AlertEntity alert, boolean isAlertMessage) {
        Pair<String, String> titleAndContent = decoratorManager.generateTitleAndContent(alert, isAlertMessage);

        EmailReceiverModel receivers = alertPolicyManager.queryEmailReceivers(alert);
        AlertMessageEntity message = new AlertMessageEntity(titleAndContent.getKey(), titleAndContent.getValue(),
                receivers.getRecipients(), alert);

        message.addParam(AbstractSender.CC_ER, receivers.getCcers());

        return message;
    }

    protected AlertMessageEntity getMessage(EmailReceiverModel receivers, Map<ALERT_TYPE, Set<AlertEntity>> alerts,
                                            boolean isAlertMessage) {

        Pair<String, String> titleAndContent = decoratorManager().generateTitleAndContent(alerts, isAlertMessage);
        AlertMessageEntity message = new AlertMessageEntity(titleAndContent.getKey(), titleAndContent.getValue(),
                receivers.getRecipients());
        message.addParam(AbstractSender.CC_ER, receivers.getCcers());

        return message;
    }

    protected void emailMessage(AlertMessageEntity message) {
        senderManager.sendAlert(AlertChannel.MAIL, message);
    }

    protected void tryMetric(Map<ALERT_TYPE, Set<AlertEntity>> alerts, boolean isAlertMessage) {
        if(alerts == null) {
            return;
        }
        String mailType = "alert";
        if(!isAlertMessage) {
            mailType = "recover";
        }
        tryMetricMail(mailType);
        for(Map.Entry<ALERT_TYPE, Set<AlertEntity>> entry : alerts.entrySet()) {
            ALERT_TYPE alertType = entry.getKey();
            tryMetricAlter(alertType, mailType);
        }

    }

    private void tryMetricMail(String mailType) {
        tryMetric("mail", null, mailType);
    }

    private void tryMetricAlter(ALERT_TYPE type, String mailType) {
        tryMetric("alert", type, mailType);
    }

    private void tryMetric(String metricType, ALERT_TYPE type, String mailType) {
        MetricData metricData = new MetricData(metricType);
        metricData.setValue(1);
        metricData.setTimestampMilli(System.currentTimeMillis());
        if(type != null) {
            metricData.addTag("type", type.name());
        }
        metricData.addTag("mailType", mailType);
        try {
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.debug("[tryMetric] fail", th);
        }
    }

    protected void tryMetric(AlertEntity alert, String metricType) {
        MetricData metricData = new MetricData(metricType, alert.getDc(), alert.getClusterId(), alert.getShardId());
        metricData.setValue(1);
        metricData.setTimestampMilli(alert.getDate().getTime());

        try {
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (Throwable th) {
            logger.debug("[tryMetric] fail", th);
        }
    }

    protected void addAlertsToAlertHolders(Set<AlertEntity> alerts, AlertEntityHolderManager holderManager) {
        try {
            lock.lock();
            for(AlertEntity alertEntity : alerts) {
                holderManager.holdAlert(alertEntity);
            }
        } finally {
            lock.unlock();
        }
    }

}