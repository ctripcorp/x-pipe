package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public class DefaultSubscribeMessageHandler extends AbstractSubscribeMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSubscribeMessageHandler.class);

    @Override
    protected Pair<String, String> doHandle(String[] subscribeChannelResponse) {
        String flag = subscribeChannelResponse[0];
        if(!Subscribe.MESSAGE_TYPE.MESSAGE.matches(flag)) {
            logger.error("[doHandle] Subscribe message not correct: {}", subscribeChannelResponse);
            return null;
        }

        return new Pair<>(subscribeChannelResponse[1], subscribeChannelResponse[2]);
    }
}
