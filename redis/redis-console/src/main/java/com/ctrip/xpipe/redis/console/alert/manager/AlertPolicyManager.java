package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;
import com.ctrip.xpipe.redis.console.alert.policy.SendToDBAAlertPolicy;
import com.ctrip.xpipe.redis.console.alert.policy.SendToRedisClusterAdminAlertPolicy;
import com.ctrip.xpipe.redis.console.alert.policy.SendToXPipeAdminAlertPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class AlertPolicyManager {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static final int EMAIL_DBA = 1 << 0;
    public static final int EMAIL_XPIPE_ADMIN = 1 << 1;
    public static final int EMAIL_CLUSTER_ADMIN = 1 << 2;


    @Autowired
    Map<String, AlertPolicy> alertPolicyMap;

    public List<AlertChannel> queryChannels(AlertEntity alert) {
        try {
            // TODO more types in the future
            if(alert.getAlertType().getAlertPolicy() <= EMAIL_CLUSTER_ADMIN) {
                return Collections.singletonList(AlertChannel.MAIL);
            }
        } catch (Exception e) {
            logger.error("[queryChannels]{}", e);
        }
        return new LinkedList<>();
    }

    public int queryRecoverMinute(AlertEntity alert) {
        try {
            List<AlertPolicy> alertPolicies = findAlertPolicies(alert);
            int result = alert.getAlertType().getRecoverTime();
            for (AlertPolicy alertPolicy : alertPolicies) {
                result = Math.max(result, alertPolicy.queryRecoverMinute(alert));
            }
            return result;
        } catch (Exception ex) {
            return 30;
        }
    }

    public int querySuspendMinute(AlertEntity alert) {
        try {
            List<AlertPolicy> alertPolicies = findAlertPolicies(alert);
            int result = 0;
            for (AlertPolicy alertPolicy : alertPolicies) {
                result = Math.max(result, alertPolicy.querySuspendMinute(alert));
            }
            return result;
        } catch (Exception ex) {
            return 5;
        }
    }

    public List<String> queryRecepients(AlertEntity alert) {
        try {
            List<AlertPolicy> alertPolicies = findAlertPolicies(alert);
            Set<String> result = new HashSet<>();
            for (AlertPolicy alertPolicy : alertPolicies) {
                result.addAll(alertPolicy.queryRecipients(alert));
            }
            return new ArrayList<>(result);
        } catch (Exception e) {
            logger.error("[queryRecepients]{}", e);
            return new LinkedList<>();
        }
    }

    public List<String> queryCCers(AlertEntity alert) {
        try {
            List<AlertPolicy> alertPolicies = findAlertPolicies(alert);
            Set<String> result = new HashSet<>();
            for (AlertPolicy alertPolicy : alertPolicies) {
                result.addAll(alertPolicy.queryCCers());
            }
            return new ArrayList<>(result);
        } catch (Exception e) {
            logger.error("[queryCCers]{}", e);
            return new LinkedList<>();
        }
    }

    private List<AlertPolicy> findAlertPolicies(AlertEntity alert) {
        List<AlertPolicy> alertPolicies = new ArrayList<>(10);
        int total = alert.getAlertType().getAlertPolicy();
        if((total & EMAIL_DBA) != 0) {
            alertPolicies.add(alertPolicyMap.get(SendToDBAAlertPolicy.ID));
        }
        if((total & EMAIL_XPIPE_ADMIN) != 0) {
            alertPolicies.add(alertPolicyMap.get(SendToXPipeAdminAlertPolicy.ID));
        }
        if((total & EMAIL_CLUSTER_ADMIN) != 0) {
            alertPolicies.add(alertPolicyMap.get(SendToRedisClusterAdminAlertPolicy.ID));
        }
        return alertPolicies;
    }

}
