package com.ctrip.xpipe.redis.console.alert.policy;

import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(SendToRedisClusterAdminAlertPolicy.ID)
public class SendToRedisClusterAdminAlertPolicy extends AbstractAlertPolicy {

    public static final String ID = "send.to.redis.cluster.admin.alert.policy";

    @Override
    public List<String> queryRecipients() {
        return new LinkedList<>();
    }

}
