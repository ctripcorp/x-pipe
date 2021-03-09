package com.ctrip.xpipe.redis.checker.alert.policy.receiver;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;

import java.util.Map;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */
public interface GroupEmailReceiver extends EmailReceiver {

    Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> getGroupedEmailReceiver(AlertEntityHolderManager alerts);

}
