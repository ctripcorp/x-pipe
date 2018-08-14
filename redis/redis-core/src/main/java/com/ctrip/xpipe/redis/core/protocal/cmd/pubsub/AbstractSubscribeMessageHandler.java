package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public abstract class AbstractSubscribeMessageHandler implements SubscribeMessageHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractSubscribeMessageHandler.class);

    @Override
    public Pair<String, String> handle(String[] subscribeChannelResponse) {
        if(subscribeChannelResponse == null || subscribeChannelResponse.length < 3) {
            logger.error("[handle] Subscribe channel message incorrect: {}", subscribeChannelResponse);
            return null;
        }
        return doHandle(subscribeChannelResponse);
    }

    protected abstract Pair<String, String> doHandle(String[] subscribeChannelResponse);
}
