package com.ctrip.xpipe.redis.console.alert.policy;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
@Component(SendToDBAAlertPolicy.ID)
public class SendToDBAAlertPolicy extends AbstractAlertPolicy {

    public static final String ID = "send.to.dba.alert.policy";

    @Override
    public List<String> queryRecipients() {
        return getDBAEmails();
    }

    @Override
    public List<String> queryCCers() {
        return getXPipeAdminEmails();
    }
}
