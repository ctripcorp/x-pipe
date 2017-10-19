package com.ctrip.xpipe.redis.console.alert.policy;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(SendToXPipeAdminAlertPolicy.ID)
public class SendToXPipeAdminAlertPolicy extends AbstractAlertPolicy {

    public static final String ID = "send.to.xpipe.admin.alert.policy";

    @Override
    public List<String> queryRecipients() {
        return getXPipeAdminEmails();
    }

}
