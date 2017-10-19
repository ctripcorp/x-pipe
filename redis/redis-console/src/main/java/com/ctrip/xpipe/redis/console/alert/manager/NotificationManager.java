package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.api.email.EmailType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.sender.EmailSender;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class NotificationManager {

    private static final Logger logger = LoggerFactory.getLogger(NotificationManager.class.getSimpleName());

    private static final int MILLIS1MINUTE = 1 * 60 * 1000;

    private BlockingQueue<AlertEntity> alerts = new LinkedBlockingDeque<>(10000);

    private Map<String, AlertEntity> unrecoveredAlerts = new ConcurrentHashMap<>(1000);

    private Map<String, AlertEntity> sendedAlerts = new ConcurrentHashMap<>(1000);

    @Autowired
    private AlertPolicyManager policyManager;

    @Autowired
    private SenderManager senderManager;

    @Autowired
    private DecoratorManager decoratorManager;

    @PostConstruct
    public void start() {
        logger.info("Alert Notification Manager started");

        XpipeThreadFactory.create("Send Alert", true).newThread(new SendAlert()).start();
        XpipeThreadFactory.create("Announce Recover", true).newThread(new AnnounceRecover()).start();
    }

    public void addAlert(String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message) {
        AlertEntity alert = new AlertEntity(hostPort, cluster, shard, message, type);
        logger.debug("[addAlert] Add Alert Entity: {}", alert);
        alerts.offer(alert);
    }

    public boolean isSuspend(String alertKey, int suspendMinute) {
        AlertEntity sendedAlert = sendedAlerts.get(alertKey);
        if (sendedAlert != null) {
            long duration = System.currentTimeMillis() - sendedAlert.getDate().getTime();
            if (duration / MILLIS1MINUTE < suspendMinute) {
                return true;
            }
        }
        return false;
    }

    protected boolean send(AlertEntity alert) {
        boolean result = false;

        String alertKey = alert.getKey();
        List<AlertChannel> channels = policyManager.queryChannels(alert);
        int suspendMinute = policyManager.querySuspendMinute(alert);

        unrecoveredAlerts.put(alertKey, alert);

        Pair<String, String> pair = decoratorManager.generateTitleAndContent(alert, true);
        String title = pair.getKey();
        String content = pair.getValue();

        if (suspendMinute > 0) {
            if (isSuspend(alertKey, suspendMinute)) {
                return false;
            } else {
                sendedAlerts.put(alertKey, alert);
            }
        }

        for (AlertChannel channel : channels) {
            List<String> receivers = policyManager.queryRecepients(alert);

            AlertMessageEntity message = new AlertMessageEntity(title, EmailType.CONSOLE_ALERT, content, receivers);
            if(channel == AlertChannel.MAIL) {
                List<String> ccers = policyManager.queryCCers(alert);
                message.addParam(EmailSender.CC_ER, ccers);
            }

            if (senderManager.sendAlert(channel, message)) {
                result = true;
            }
        }

        return result;
    }

    protected boolean sendRecoveryMessage(AlertEntity alert, String currentMinute) {

        List<AlertChannel> channels = policyManager.queryChannels(alert);
        String prefix = "<entry><htmlContent><![CDATA[";
        String suffix = "]]></htmlContent></entry>";
        for (AlertChannel channel : channels) {
            String title = "[告警恢复] [告警类型 " + alert.getAlertType() + "][" + alert.getMessage() + "]";
            String content = prefix + "[告警已恢复][恢复时间]" + currentMinute + suffix;
            List<String> receivers = policyManager.queryRecepients(alert);

            AlertMessageEntity message = new AlertMessageEntity(title, EmailType.CONSOLE_ALERT, content, receivers);
            if(channel == AlertChannel.MAIL) {
                List<String> ccers = policyManager.queryCCers(alert);
                message.addParam(EmailSender.CC_ER, ccers);
            }

            if (senderManager.sendAlert(channel, message)) {
                return true;
            }
        }

        return false;
    }

    private class SendAlert implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    AlertEntity alert = alerts.poll(5, TimeUnit.MILLISECONDS);

                    if (alert != null) {
                        send(alert);
                    }
                } catch (Throwable e) {
                    logger.error("Unexpected error when sending alert", e);
                }
            }
        }
    }

    protected class AnnounceRecover implements Runnable {
        @Override
        public void run() {
            while (true) {
                long current = System.currentTimeMillis();
                String currentStr = DateTimeUtils.currentTimeAsString();
                List<String> recoveredItems = new LinkedList<>();

                for (Map.Entry<String, AlertEntity> entry : unrecoveredAlerts.entrySet()) {
                    try {
                        String key = entry.getKey();
                        AlertEntity alert = entry.getValue();
                        int recoverMinute = policyManager.queryRecoverMinute(alert);
                        long alertTime = alert.getDate().getTime();
                        int alreadyMinutes = (int) ((current - alertTime) / MILLIS1MINUTE);

                        if (alreadyMinutes >= recoverMinute) {
                            recoveredItems.add(key);
                            sendRecoveryMessage(alert, currentStr);
                        }
                    } catch (Exception e) {
                        logger.error("{}", e);
                    }
                }

                for (String key : recoveredItems) {
                    unrecoveredAlerts.remove(key);
                }

                recoveredItems = null;

                long duration = System.currentTimeMillis() - current;
                if (duration < MILLIS1MINUTE) {
                    long lackMills = MILLIS1MINUTE - duration;

                    try {
                        TimeUnit.MILLISECONDS.sleep(lackMills);
                    } catch (InterruptedException e) {
                        logger.error("{}", e);
                    }
                }
            }
        }
    }
}
