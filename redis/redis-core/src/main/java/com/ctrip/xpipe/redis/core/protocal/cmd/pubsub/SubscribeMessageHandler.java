package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.tuple.Pair;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public interface SubscribeMessageHandler {
    Pair<String, String> handle(String[] channelResponse);
}
