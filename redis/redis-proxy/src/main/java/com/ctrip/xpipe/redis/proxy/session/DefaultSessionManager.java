package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultSessionManager implements SessionManager {

    private Tunnel tunnel;

    private volatile Session frontend;

    private volatile Session backend;

    public DefaultSessionManager(Tunnel tunnel) {
        this.tunnel = tunnel;
    }

    @Override
    public Tunnel tunnel() {
        return tunnel;
    }

    @Override
    public Session frontend() {
        if(frontend == null) {
            synchronized (this) {
                if(frontend == null) {
                    frontend = new DefaultSession(tunnel, SESSION_TYPE.FRONTEND, tunnel().frontendChannel());
                }
            }
        }
        return frontend;
    }

    @Override
    public Session backend() {
        if(frontend == null) {
            synchronized (this) {
                if(frontend == null) {
                    frontend = new DefaultSession(tunnel, SESSION_TYPE.BACKEND, tunnel().getNextJump());
                }
            }
        }
        return frontend;
    }

    @Override
    public Session getOppositeSession(Session src) {
        return null;
    }

    @Override
    public List<Session> getSessions() {
        return null;
    }
}
