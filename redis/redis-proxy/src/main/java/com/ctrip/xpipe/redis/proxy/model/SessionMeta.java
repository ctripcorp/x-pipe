package com.ctrip.xpipe.redis.proxy.model;

import com.ctrip.xpipe.redis.proxy.session.SESSION_TYPE;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class SessionMeta {

    private SESSION_TYPE type;

    private Channel channel;
}
