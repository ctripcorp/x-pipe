package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface SessionManager {

    Tunnel tunnel();

    Session frontend();

    Session backend();

    Session getOppositeSession(Session src);

    List<Session> getSessions();
}
