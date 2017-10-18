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
@Component(QuorumDownFailAlertPolicy.ID)
public class QuorumDownFailAlertPolicy extends AbstractAlertPolicy {

    public static final String ID = "quorum.down.fail.alert.policy";

    @Override
    public List<String> queryRecipients() {
        return getXPipeAdminEmails();
    }

}
