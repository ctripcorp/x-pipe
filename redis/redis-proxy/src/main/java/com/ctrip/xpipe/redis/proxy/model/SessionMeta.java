package com.ctrip.xpipe.redis.proxy.model;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.SessionState;
import com.ctrip.xpipe.utils.ChannelUtil;

import java.io.Serializable;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class SessionMeta implements Serializable {

    private long id;

    private String type;

    private String channel;

    private String endpoint;

    private String state;

    public SessionMeta(Session session, ProxyEndpoint endpoint, SessionState state) {
        this.id = session.getSessionId();
        this.type = session.getSessionType().name();
        this.channel = ChannelUtil.getDesc(session.getChannel());
        this.endpoint = endpoint.getUri();
        this.state = state.name();
    }

    public SessionMeta(String type, long sessionId, String channel, String endpoint, String state) {
        this.type = type;
        this.id = sessionId;
        this.channel = channel;
        this.endpoint = endpoint;
        this.state = state;
    }

    public String getType() {
        return type;
    }

    public SessionMeta setType(String type) {
        this.type = type;
        return this;
    }

    public String getChannel() {
        return channel;
    }

    public SessionMeta setChannel(String channel) {
        this.channel = channel;
        return this;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public SessionMeta setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public String getState() {
        return state;
    }

    public SessionMeta setState(String state) {
        this.state = state;
        return this;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
