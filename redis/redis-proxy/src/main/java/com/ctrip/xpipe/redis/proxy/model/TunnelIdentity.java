package com.ctrip.xpipe.redis.proxy.model;

import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * Jul 25, 2018
 */
public class TunnelIdentity {

    private Channel frontend;

    private Channel backend;

    private String destination;

    private String source;

    private String identityString;

    public TunnelIdentity(Channel frontend, String destination, String source) {
        this.frontend = frontend;
        this.destination = destination;
        this.source = source;
    }

    public Channel getFrontend() {
        return frontend;
    }

    public TunnelIdentity setFrontend(Channel frontend) {
        this.frontend = frontend;
        return this;
    }

    public Channel getBackend() {
        return backend;
    }

    public TunnelIdentity setBackend(Channel backend) {
        this.backend = backend;
        return this;
    }

    public String getDestination() {
        return destination;
    }

    public TunnelIdentity setDestination(String destination) {
        this.destination = destination;
        return this;
    }

    @Override
    public String toString() {
        return identityString != null ? identityString : String.format("%s-%s-%s-%s",
                    source, ChannelUtil.getRemoteAddr(frontend), ChannelUtil.getDesc(backend), destination);
    }
}
