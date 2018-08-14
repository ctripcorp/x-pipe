package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public interface SubscribeListener {
    void message(String channel, String message);
}
