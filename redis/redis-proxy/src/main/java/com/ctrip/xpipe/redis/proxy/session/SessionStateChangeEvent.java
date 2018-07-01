package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.api.observer.Event;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public class SessionStateChangeEvent implements Event {

    private SessionState previous;

    private SessionState current;

    public SessionStateChangeEvent(SessionState previous, SessionState current) {
        this.previous = previous;
        this.current = current;
    }

    public SessionState getPrevious() {
        return previous;
    }

    public SessionState getCurrent() {
        return current;
    }
}
