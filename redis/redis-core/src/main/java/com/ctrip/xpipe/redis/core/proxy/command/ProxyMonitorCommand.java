package com.ctrip.xpipe.redis.core.proxy.command;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 26, 2018
 */
public class ProxyMonitorCommand extends AbstractProxyCommand<Object> {
    public ProxyMonitorCommand(String host, int port, ScheduledExecutorService scheduled) {
        super(host, port, scheduled);
    }

    @Override
    protected Object format(Object payload) {
        return null;
    }

    @Override
    public ByteBuf getRequest() {
        return null;
    }
}
