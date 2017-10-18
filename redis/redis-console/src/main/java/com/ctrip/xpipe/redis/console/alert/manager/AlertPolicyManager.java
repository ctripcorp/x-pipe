package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;
import com.ctrip.xpipe.redis.console.alert.policy.DefaultAlertPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class AlertPolicyManager {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private AlertPolicy defaultAlertPolicy = new DefaultAlertPolicy();

    @Autowired
    Map<String, AlertPolicy> alertPolicyMap;

    public List<AlertChannel> queryChannels(AlertEntity alert) {
        try {
            String alertPolicyId = alert.getAlertType().getAlertPolicyId();
            AlertPolicy alertPolicy = alertPolicyMap.getOrDefault(alertPolicyId, defaultAlertPolicy);
            return alertPolicy.queryChannels();
        } catch (Exception e) {
            logger.error("[queryChannels]{}", e);
            return new LinkedList<>();
        }
    }

    public int queryRecoverMinute(AlertEntity alert) {
        try {
            return findAlertPolicy(alert).queryRecoverMinute();
        } catch (Exception ex) {
            return 30;
        }
    }

    public int querySuspendMinute(AlertEntity alert) {
        try {
            return findAlertPolicy(alert).querySuspendMinute();
        } catch (Exception ex) {
            return 30;
        }
    }

    public List<String> queryRecepients(AlertEntity alert) {
        try {
            return findAlertPolicy(alert).queryRecipients();
        } catch (Exception e) {
            logger.error("[queryRecepients]{}", e);
            return new LinkedList<>();
        }
    }

    public List<String> queryCCers(AlertEntity alert) {
        try {
            AlertPolicy alertPolicy = findAlertPolicy(alert);
            return alertPolicy.queryCCers();
        } catch (Exception e) {
            logger.error("[queryCCers]{}", e);
            return new LinkedList<>();
        }
    }

    private AlertPolicy findAlertPolicy(AlertEntity alert) {
        String alertPolicyId = alert.getAlertType().getAlertPolicyId();
        return alertPolicyMap.getOrDefault(alertPolicyId, defaultAlertPolicy);
    }
}
