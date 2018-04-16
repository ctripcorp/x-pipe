package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public class SubscribeCommand extends AbstractSubscribe {

    public SubscribeCommand(String host, int port, ScheduledExecutorService scheduled, String channel) {
        super(host, port, scheduled, channel, MESSAGE_TYPE.MESSAGE);
    }

    @Override
    public void doUnsubscribe() { }

    @Override
    public String getName() {
        return "subscribe";
    }
}
