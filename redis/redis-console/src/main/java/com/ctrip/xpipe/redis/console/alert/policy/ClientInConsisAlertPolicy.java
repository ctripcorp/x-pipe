package com.ctrip.xpipe.redis.console.alert.policy;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component(ClientInConsisAlertPolicy.ID)
public class ClientInConsisAlertPolicy extends AbstractAlertPolicy {

    public static final String ID = "client.inconsis.alert.policy";

    @Override
    public List<String> queryRecipients() {
        return getDBAEmails();
    }

    @Override
    public List<String> queryCCers() {
        return getXPipeAdminEmails();
    }
}
