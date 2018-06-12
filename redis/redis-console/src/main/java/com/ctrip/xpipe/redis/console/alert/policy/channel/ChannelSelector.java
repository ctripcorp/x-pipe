package com.ctrip.xpipe.redis.console.alert.policy.channel;

import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;

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
