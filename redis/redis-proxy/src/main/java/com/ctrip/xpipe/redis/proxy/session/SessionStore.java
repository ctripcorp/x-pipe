package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.channel.Channel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface SessionStore extends Lifecycle, Releasable {

    Tunnel tunnel();

    Session session(Channel channel);

    Session frontend();

    Session backend();

    Session getOppositeSession(Session src);

    List<Session> getSessions();
}
