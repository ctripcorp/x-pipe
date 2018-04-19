package com.ctrip.xpipe.redis.console.alert.policy.timing;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;

import java.util.function.LongSupplier;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface TimeSlotControl extends AlertPolicy {

    long durationMilli(AlertEntity alert);

    void mark(ALERT_TYPE alertType, LongSupplier checkInterval);

    @Override
    default boolean supports(Class<? extends AlertPolicy> clazz) {
        return clazz.isAssignableFrom(TimeSlotControl.class);
    }
}
