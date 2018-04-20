package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface EmailReceiver extends AlertPolicy {

    EmailReceiverModel receivers(AlertEntity alert);

    int EMAIL_DBA = 1 << 0;
    int EMAIL_XPIPE_ADMIN = 1 << 1;
    int EMAIL_CLUSTER_ADMIN = 1 << 2;

    @Override
    default boolean supports(Class<? extends AlertPolicy> clazz) {
        return clazz.isAssignableFrom(EmailReceiver.class);
    }
}
