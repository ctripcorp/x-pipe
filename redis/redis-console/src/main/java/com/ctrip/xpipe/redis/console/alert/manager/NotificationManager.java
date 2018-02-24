package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.api.email.EmailType;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.sender.EmailSender;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class NotificationManager {

    private static final Logger logger = LoggerFactory.getLogger(NotificationManager.class.getSimpleName());

    private static final int MILLIS1MINUTE = 60 * 1000;

    private BlockingQueue<AlertEntity> alerts = new LinkedBlockingDeque<>(10000);

    private Map<String, AlertEntity> unrecoveredAlerts = new ConcurrentHashMap<>(1000);

    private Map<String, AlertEntity> sendedAlerts = new ConcurrentHashMap<>(1000);

    private Map<ALERT_TYPE, Set<AlertEntity>> scheduledAlerts = new ConcurrentHashMap<>(1000);

    @Autowired
    private AlertPolicyManager policyManager;

    @Autowired
    private SenderManager senderManager;

    @Autowired
    private DecoratorManager decoratorManager;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService schedule;


    private Thread alertSender;

    private Thread recoverAnnoucer;

    @PreDestroy
    public void shutdown() {
        if(alertSender != null) {
            alertSender.interrupt();
        }
        if(recoverAnnoucer != null) {
            recoverAnnoucer.interrupt();
        }
    }

    @PostConstruct
    public void start() {
        logger.info("Alert Notification Manager started");

        alertSender = XpipeThreadFactory.create("NotificationManager-alert-sender", true)
                .newThread(new SendAlert());
        recoverAnnoucer = XpipeThreadFactory.create("NotificationManager-recover-annoucer", true)
                .newThread(new AnnounceRecover());

        alertSender.start();
        recoverAnnoucer.start();

        schedule.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() {
                try {
                    senderManager.sendAlerts(scheduledAlerts);
                    scheduledAlerts = new ConcurrentHashMap<>();
                } catch (Exception e) {
                    logger.error("[start][schedule]{}", e);
                }
            }
        }, 1, 30, TimeUnit.MINUTES);
    }


    public void addAlert(String dc, String cluster, String shard, HostPort hostPort, ALERT_TYPE type, String message) {
        AlertEntity alert = new AlertEntity(hostPort, dc, cluster, shard, message, type);
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

        // Skip the existing alerts, and report them once upon time
        if(unrecoveredAlerts.containsKey(alertKey)) {
            scheduledAlerts.putIfAbsent(alert.getAlertType(), new HashSet<>());
            scheduledAlerts.get(alert.getAlertType()).add(alert);
            unrecoveredAlerts.put(alertKey, alert);
            return false;
        }

        unrecoveredAlerts.put(alertKey, alert);

        if (suspendMinute > 0) {
            if (isSuspend(alertKey, suspendMinute)) {
                return false;
            } else {
                sendedAlerts.put(alertKey, alert);
            }
        }

        Pair<String, String> pair = decoratorManager.generateTitleAndContent(alert, true);
        String title = pair.getKey();
        String content = pair.getValue();

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

    protected boolean sendRecoveryMessage(AlertEntity alert) {

        List<AlertChannel> channels = policyManager.queryChannels(alert);
        Pair<String, String> pair = decoratorManager.generateTitleAndContent(alert, false);
        String title = pair.getKey(), content = pair.getValue();
        for (AlertChannel channel : channels) {

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
            while (!Thread.currentThread().isInterrupted()) {

                try {
                    long current = System.currentTimeMillis();
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
                                if(announceRecovery(alert.getAlertType())) {
                                    sendRecoveryMessage(alert);
                                }
                            }
                        } catch (Exception e) {
                            logger.error("{}", e);
                        }
                    }

                    for (String key : recoveredItems) {
                        AlertEntity alertEntity = unrecoveredAlerts.remove(key);
                        Set<AlertEntity> set = scheduledAlerts.getOrDefault(alertEntity.getAlertType(), null);
                        if (set != null && !set.isEmpty()) {
                            set.remove(alertEntity);
                        }
                    }

                    long duration = System.currentTimeMillis() - current;
                    if (duration < MILLIS1MINUTE) {
                        long lackMills = MILLIS1MINUTE - duration;

                        try {
                            TimeUnit.MILLISECONDS.sleep(lackMills);
                        } catch (InterruptedException e) {
                            logger.error("{}", e);
                        }
                    }
                } catch (Exception e) {
                    logger.error("[AnnounceRecover][run] {}", e);
                }

            }
        }

        private boolean announceRecovery(ALERT_TYPE alertType) {
            for(ALERT_TYPE type : unAnnocedRecovers) {
                if(type == alertType)
                    return false;
            }
            return true;
        }

        private ALERT_TYPE[] unAnnocedRecovers = {ALERT_TYPE.MARK_INSTANCE_DOWN,
                ALERT_TYPE.MARK_INSTANCE_UP, ALERT_TYPE.QUORUM_DOWN_FAIL,
                ALERT_TYPE.ALERT_SYSTEM_OFF, ALERT_TYPE.SENTINEL_AUTO_PROCESS_OFF,
                ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, ALERT_TYPE.SENTINEL_MONITOR_REDUNDANT_REDIS
        };
    }
}
