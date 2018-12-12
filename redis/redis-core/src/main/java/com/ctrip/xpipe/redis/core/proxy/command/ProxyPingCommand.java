package com.ctrip.xpipe.redis.core.proxy.command;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class ProxyPingCommand extends AbstractProxyCommand<ProxyPongEntity> {

    private static final String PING_PREFIX = String.format("%s %s", ProxyProtocol.KEY_WORD, "PING");

    private static final String PONG_PREFIX = String.format("%s %s", ProxyProtocol.KEY_WORD, "PONG");

    private ProxyEndpoint target;

    private static final int ONE_RTT_TIMEOUT = 2000;

    private int timeout = ONE_RTT_TIMEOUT;


    public ProxyPingCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    public ProxyPingCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                            int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
    }

    public ProxyPingCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                            ProxyEndpoint target) {
        super(clientPool, scheduled);
        this.target = target;
    }

    public ProxyPingCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                            int commandTimeoutMilli, ProxyEndpoint target) {
        super(clientPool, scheduled, commandTimeoutMilli);
        this.target = target;
    }

    @Override
    protected ProxyPongEntity format(Object payload) {
        String response = payloadToString(payload);
        validResponse(response);
        return formatResponse(response);
    }

    @Override
    public ByteBuf getRequest() {
        String command = PING_PREFIX;
        if(target != null) {
            command = String.format("%s %s", PING_PREFIX, target.getUri());
        }
        return new SimpleStringParser(command).format();
    }

    private void validResponse(String response) {
        if(!response.startsWith(PONG_PREFIX)) {
            throw new IllegalArgumentException("PingCommand Response not valid as: " + response);
        }
    }

    private ProxyPongEntity formatResponse(String response) {
        String[] elements = StringUtil.splitRemoveEmpty(AbstractProxyOptionParser.WHITE_SPACE, response);
        if(elements.length == 3) {
            return new ProxyPongEntity(HostPort.fromString(elements[2]));
        } else if(elements.length == 5) {
            timeout = 2 * ONE_RTT_TIMEOUT;
            return new ProxyPongEntity(HostPort.fromString(elements[2]), HostPort.fromString(elements[3]), Long.parseLong(elements[4]));
        }
        throw new IllegalArgumentException("Proxy Pong Response not valid as: " + response);
    }

    @Override
    protected boolean logRequest() {
        return false;
    }

    @Override
    protected boolean logResponse() {
        return false;
    }

    @Override
    public int getCommandTimeoutMilli() {
        return timeout;
    }
}
