package com.ctrip.xpipe.redis.checker.alert.policy.channel;

import com.ctrip.xpipe.redis.checker.alert.AlertChannel;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.policy.AlertPolicy;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface ChannelSelector extends AlertPolicy {

    List<AlertChannel> alertChannels(AlertEntity alert);

    @Override
    default boolean supports(Class<? extends AlertPolicy> clazz) {
        return clazz.isAssignableFrom(ChannelSelector.class);
    }
}
