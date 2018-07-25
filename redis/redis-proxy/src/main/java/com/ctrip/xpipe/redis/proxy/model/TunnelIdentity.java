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

    private String destionation;

    public TunnelIdentity(Channel frontend, String destionation) {
        this.frontend = frontend;
        this.destionation = destionation;
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

    public String getDestionation() {
        return destionation;
    }

    public TunnelIdentity setDestionation(String destionation) {
        this.destionation = destionation;
        return this;
    }

    @Override
    public String toString() {
        return ChannelUtil.getRemoteAddr(frontend) + "-" + ChannelUtil.getDesc(backend) + "-" + destionation;
    }
}
