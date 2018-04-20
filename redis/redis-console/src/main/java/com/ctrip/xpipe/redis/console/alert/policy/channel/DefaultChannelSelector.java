package com.ctrip.xpipe.redis.console.alert.policy.channel;

import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.google.common.collect.Lists;

import java.util.List;

import static com.ctrip.xpipe.redis.console.alert.policy.receiver.EmailReceiver.*;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public class DefaultChannelSelector implements ChannelSelector {

    @Override
    public List<AlertChannel> alertChannels(AlertEntity alert) {

        if(activeEmailChannel(alert)) {
            return Lists.newArrayList(AlertChannel.MAIL);
        }

        return null;
    }

    private boolean activeEmailChannel(AlertEntity alert) {
        int emailChannel = EMAIL_DBA | EMAIL_XPIPE_ADMIN | EMAIL_CLUSTER_ADMIN;
        return (alert.getAlertType().getAlertMethod() & emailChannel) != 0;
    }
}
