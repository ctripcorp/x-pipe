package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.api.email.EmailType;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.policy.SendToDBAAlertPolicy;
import com.ctrip.xpipe.redis.console.alert.sender.Sender;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class SenderManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    Map<String, Sender> senders;

    @Autowired
    DecoratorManager decoratorManager;

    @Autowired
    SendToDBAAlertPolicy policy;

    @Autowired
    ConfigService configService;

    private boolean shouldAlert(ALERT_TYPE type) {
        return configService.isAlertSystemOn() || type == ALERT_TYPE.ALERT_SYSTEM_OFF;
    }

    public Sender querySender(String id) {
        return senders.get(id);
    }

    public boolean sendAlert(AlertChannel channel, AlertMessageEntity message) {
        if(message.getAlert() != null && !shouldAlert(message.getAlert().getAlertType())) {
            logger.warn("[sendAlert] Alert System is off, won't send");
            return false;
        }
        String channelId = channel.getId();
        Sender sender = senders.get(channelId);
        try {
            boolean result = sender.send(message);
            logger.info("[sendAlert] Channel: {}, message: {}, send out: {}", channel, message.getTitle(), result);
            return result;
        } catch (Exception e) {
            logger.error("[sendAlert] {}", e);
            return false;
        }
    }

    /*************************************************************************
     * Ugly implement here, because a unit test is needed for a rarely occurred bug
     * that will cause an mail was sent with specified alert type while missing alert details
     * Which, obviously was caused by multi-thread, and the concurrenthashmap is not capable of
     * controlling the situation that one "thief thread" trying to remove stuffs from its value(set, list, etc.)
     *
     * */
    List<Map<ALERT_TYPE, Set<AlertEntity>>> getGroupedAlerts(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        List<Map<ALERT_TYPE, Set<AlertEntity>>> result = new ArrayList<>(3);
        if(alerts == null || alerts.isEmpty())
            return null;

        Map<ALERT_TYPE, Set<AlertEntity>> sendToDBA = new HashMap<>();
        Map<ALERT_TYPE, Set<AlertEntity>> sendToXPipeAdmin = new HashMap<>();
        Map<ALERT_TYPE, Set<AlertEntity>> sendToClusterAdmin = new HashMap<>();
        for(ALERT_TYPE type : alerts.keySet()) {
            Set<AlertEntity> alertEntities = new HashSet<>(alerts.get(type));
            if(alertEntities == null || alertEntities.isEmpty())
                continue;

            logger.info("[email_dba] {}, {}, {}", alertEntities, alertEntities.isEmpty(), alertEntities.size());
            if((type.getAlertPolicy() & EMAIL_DBA) != 0
                    && shouldAlert(type)) {
                sendToDBA.put(type, alertEntities);
            }
            if((type.getAlertPolicy() & EMAIL_XPIPE_ADMIN) != 0) {
                sendToXPipeAdmin.put(type, alertEntities);
            }
            if((type.getAlertPolicy() & EMAIL_CLUSTER_ADMIN) != 0
                    && shouldAlert(type)) {
                sendToClusterAdmin.put(type, alertEntities);
            }
        }

        result.add(sendToDBA);
        result.add(sendToXPipeAdmin);
        result.add(sendToClusterAdmin);
        return result;
    }

    public boolean sendAlerts(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        List<Map<ALERT_TYPE, Set<AlertEntity>>> groupedAlerts = getGroupedAlerts(alerts);
        if(groupedAlerts == null || groupedAlerts.isEmpty())
            return false;

        Map<ALERT_TYPE, Set<AlertEntity>> sendToDBA = groupedAlerts.get(0);
        Map<ALERT_TYPE, Set<AlertEntity>> sendToXPipeAdmin = groupedAlerts.get(1);
        Map<ALERT_TYPE, Set<AlertEntity>> sendToClusterAdmin = groupedAlerts.get(2);
        try {
            return sendToDBA(sendToDBA) &&
            sendToXPipeAdmin(sendToXPipeAdmin) &&
            sendToClusterAdmin(sendToClusterAdmin);
        } catch (Exception e) {
            logger.error("[sendAlerts] {}", e);
            return false;
        }
    }
    /***********************************************************************************/


    private boolean sendToClusterAdmin(Map<ALERT_TYPE, Set<AlertEntity>> sendToClusterAdmin) {
        List<String> emails = new LinkedList<>();
        return sendBatchEmails(sendToClusterAdmin, emails);
    }


    private boolean sendToXPipeAdmin(Map<ALERT_TYPE, Set<AlertEntity>> sendToXPipeAdmin) {
        List<String> emails = policy.getXPipeAdminEmails();
        return sendBatchEmails(sendToXPipeAdmin, emails);
    }

    private boolean sendToDBA(Map<ALERT_TYPE, Set<AlertEntity>> sendToDBA) {
        List<String> emails = policy.getDBAEmails();
        return sendBatchEmails(sendToDBA, emails);
    }
    
    private boolean sendBatchEmails(Map<ALERT_TYPE, Set<AlertEntity>> alerts, List<String> receivers) {
        if(alerts == null || alerts.isEmpty() || receivers == null || receivers.isEmpty()) {
            return true;
        }
        Pair<String, String> pair = decoratorManager.generateTitleAndContent(alerts);
        String title = pair.getKey(), content = pair.getValue();
        AlertMessageEntity message = new AlertMessageEntity(title, EmailType.CONSOLE_ALERT, content, receivers, null);
        return sendAlert(AlertChannel.MAIL, message);
    }
}
