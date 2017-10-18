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
@Component(MigrationManyUnfinishedAlertPolicy.ID)
public class MigrationManyUnfinishedAlertPolicy extends AbstractAlertPolicy {

    public static final String ID = "migration.many.unfinished.alert.policy";

    @Override
    public List<String> queryRecipients() {
        return getXPipeAdminEmails();
    }

}
