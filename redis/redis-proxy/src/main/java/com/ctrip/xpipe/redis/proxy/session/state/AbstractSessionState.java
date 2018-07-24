package com.ctrip.xpipe.redis.proxy.session.state;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public abstract class AbstractSessionState implements SessionState {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractSessionState.class);

    protected Session session;

    public AbstractSessionState(Session session) {
        this.session = session;
    }

    @Override
    public SessionState nextAfterSuccess() {
        SessionState next = doNextAfterSuccess();
        logger.debug("[nextAfterSuccess] current: {}, next: {}", this.name(), next.name());
        return next;
    }

    protected abstract SessionState doNextAfterSuccess();

    @Override
    public SessionState nextAfterFail() {
        SessionState next = doNextAfterFail();
        logger.debug("[nextAfterFail] current: {}, next: {}", this.name(), next.name());
        return next;
    }

    protected abstract SessionState doNextAfterFail();

    @Override
    public boolean isValidNext(SessionState sessionState) {
        return sessionState.equals(this.nextAfterFail()) ||
                sessionState.equals(this.nextAfterSuccess());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(!(obj instanceof SessionState)) {
            return false;
        }
        SessionState other = (SessionState) obj;
        return this.name().equals(other.name());
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(session);
    }

    @Override
    public String toString() {
        return name();
    }
}
