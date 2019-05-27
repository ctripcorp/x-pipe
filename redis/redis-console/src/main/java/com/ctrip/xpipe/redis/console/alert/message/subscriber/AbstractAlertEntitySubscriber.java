package com.ctrip.xpipe.redis.console.alert.message.subscriber;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.console.alert.manager.DecoratorManager;
import com.ctrip.xpipe.redis.console.alert.manager.SenderManager;
import com.ctrip.xpipe.redis.console.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.console.alert.message.AlertEntitySubscriber;
import com.ctrip.xpipe.redis.console.alert.policy.receiver.EmailReceiverModel;
import com.ctrip.xpipe.redis.console.alert.sender.AbstractSender;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.job.event.EventBus;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public abstract class AbstractAlertEntitySubscriber implements AlertEntitySubscriber {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractAlertEntitySubscriber.class);

    protected final Lock lock = new ReentrantLock();

    @Autowired
    private DecoratorManager decoratorManager;

    @Autowired
    private SenderManager senderManager;

    @Autowired
    private AlertPolicyManager alertPolicyManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    protected ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
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
        logger.debug("process alert: {}", alert);
        doProcessAlert(alert);
    }

    protected abstract void doProcessAlert(AlertEntity alert);

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

    protected long recoveryMilli(AlertEntity alert) {
        return alertPolicyManager.queryRecoverMilli(alert);
    }

    protected boolean alertRecovered(AlertEntity alert) {
        long recoveryMilli = recoveryMilli(alert);
        long expectedRecoverMilli = recoveryMilli + alert.getDate().getTime();
        if(expectedRecoverMilli <= System.currentTimeMillis()) {
            logger.warn("[alertRecovered] alert: {}, expected: {}, now: {}", DateTimeUtils.timeAsString(alert.getDate()),
                    DateTimeUtils.timeAsString(expectedRecoverMilli), DateTimeUtils.currentTimeAsString());
            return true;
        }
        return false;
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

    protected ConsoleConfig consoleConfig() {
        return consoleConfig;
    }

    protected AlertPolicyManager alertPolicyManager() {
        return alertPolicyManager;
    }

    protected DecoratorManager decoratorManager() {
        return decoratorManager;
    }
}
