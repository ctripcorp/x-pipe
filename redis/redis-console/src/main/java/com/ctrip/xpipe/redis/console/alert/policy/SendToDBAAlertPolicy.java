package com.ctrip.xpipe.redis.console.alert.policy;


import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
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
    public List<String> queryRecipients(AlertEntity alert) {
        if(!shouldAlert(alert.getAlertType())) {
            return new ArrayList<>();
        }
        return getDBAEmails();
    }

    @Override
    public List<String> queryCCers() {
        return getXPipeAdminEmails();
    }
}
