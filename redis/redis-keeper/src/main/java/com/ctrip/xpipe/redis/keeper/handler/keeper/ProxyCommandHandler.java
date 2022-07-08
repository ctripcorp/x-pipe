package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class ProxyCommandHandler extends AbstractCommandHandler {

    private static final String WHITE_SPACE = " ";

    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) {
        String proxyProtocol = restructCommand(args);
        logger.info("[doHandle]receive proxy protocol: {}", proxyProtocol);

        ProxyConnectProtocol protocol = (ProxyConnectProtocol) new DefaultProxyConnectProtocolParser().read(proxyProtocol);
        protocol.recordForwardFor((InetSocketAddress) redisClient.channel().remoteAddress());
        String forwardFor = protocol.getForwardFor();

        Endpoint srcEndpoint = getSourceEndpoint(forwardFor);
        redisClient.setClientIpAddress(srcEndpoint.getHost());
        redisClient.setClientEndpoint(new ProxyEnabledEndpoint(srcEndpoint.getHost(), srcEndpoint.getPort(), protocol));
    }

    private String restructCommand(String[] args) {
        StringBuilder sb = new StringBuilder("Proxy");
        for(String arg : args) {
            sb.append(WHITE_SPACE).append(arg);
        }
        return sb.toString();
    }

    private Endpoint getSourceEndpoint(String forwardFor) {
        String[] pathStrs = forwardFor.split("\\h");
        if(pathStrs.length < 2) {
            return new DefaultEndPoint();
        }
        String hostAndPort = pathStrs[1];
        HostPort hostPort = HostPort.fromString(hostAndPort);
        return new DefaultEndPoint(hostPort.getHost(), hostPort.getPort());
    }

    @Override
    public String[] getCommands() {
        return new String[]{"proxy"};
    }

    @Override
    public boolean support(RedisServer server) {
        return server instanceof RedisKeeperServer;
    }

}
