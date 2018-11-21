package com.ctrip.xpipe.redis.core.proxy.command;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public abstract class AbstractProxyCommand<T> extends AbstractRedisCommand<T> implements ProxyCommand<T> {

    private static final int DEFAULT_PROXY_COMMAND_TIMEOUT = 10000;

    public AbstractProxyCommand(String host, int port, ScheduledExecutorService scheduled) {
        super(host, port, scheduled, DEFAULT_PROXY_COMMAND_TIMEOUT);
    }

    public AbstractProxyCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled, DEFAULT_PROXY_COMMAND_TIMEOUT);
    }

    public AbstractProxyCommand(String host, int port, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
        super(host, port, scheduled, commandTimeoutMilli);
    }

    public AbstractProxyCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
    }
}
