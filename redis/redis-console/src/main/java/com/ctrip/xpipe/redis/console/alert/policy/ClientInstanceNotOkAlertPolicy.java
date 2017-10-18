package com.ctrip.xpipe.redis.console.alert.policy;

import com.ctrip.xpipe.redis.console.alert.sender.EmailSender;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component(ClientInstanceNotOkAlertPolicy.ID)
public class ClientInstanceNotOkAlertPolicy extends AbstractAlertPolicy {

    public static final String ID = "client.instance.not.ok.alert.policy";

    @Override
    public List<String> queryRecipients() {
        return getDBAEmails();
    }

    @Override
    public List<String> queryCCers() {
        return getXPipeAdminEmails();
    }
}
