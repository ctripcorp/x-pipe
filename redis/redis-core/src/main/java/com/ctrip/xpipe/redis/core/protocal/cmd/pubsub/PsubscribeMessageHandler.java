package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public class PsubscribeMessageHandler extends AbstractSubscribeMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(PsubscribeMessageHandler.class);

    @Override
    protected Pair<String, String> doHandle(String[] subscribeChannelResponse) {
        String flag = subscribeChannelResponse[0];
        if(!Subscribe.MESSAGE_TYPE.PMESSAGE.matches(flag)) {
            logger.error("[doHandle] PSubscribe message not correct: {}", flag);
            return null;
        }
        logger.debug("[doHandle] Raw channel {} matches {}, and message: {}", subscribeChannelResponse[1],
                subscribeChannelResponse[2], subscribeChannelResponse[3]);

        return new Pair<>(subscribeChannelResponse[2], subscribeChannelResponse[3]);
    }
}
