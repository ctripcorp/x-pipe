package com.ctrip.xpipe.redis.proxy.handler.response;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.command.ProxyPingCommand;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.Channel;

public class ProxyPingHandler extends AbstractProxyProtocolOptionHandler {

    private ResourceManager resourceManager;

    public static final int INFINITE = -99999999;

    public ProxyPingHandler(ResourceManager resourceManager) {
        super(()->resourceManager.getProxyConfig().getResponseTimeout());
        this.resourceManager = resourceManager;
    }

    @Override
    public PROXY_OPTION getOption() {
        return PROXY_OPTION.PING;
    }

    @Override
    protected void doHandle(Channel channel, String[] content) {
        if(content.length < 1) {
            new SimplePingResponser().response(channel);
        } else {
            new ForwardPingResponser(content[0]).response(channel);
        }
    }

    private class SimplePingResponser implements Responser {

        @Override
        public void response(Channel channel) {
            HostPort direct = HostPort.fromString(ChannelUtil.getSimpleIpport(channel.localAddress()));
            ProxyPongEntity pong = new ProxyPongEntity(direct);
            channel.writeAndFlush(pong.output());
        }
    }

    private class ForwardPingResponser implements Responser {

        private ProxyEndpoint target;

        private HostPort direct;

        public ForwardPingResponser(String uri) {
            target = new DefaultProxyEndpoint(uri);
            direct = new HostPort(target.getHost(), target.getPort());
        }

        @Override
        public void response(Channel channel) {
            ProxyPingCommand command = new ProxyPingCommand(resourceManager.getKeyedObjectPool().getKeyPool(target),
                    resourceManager.getGlobalSharedScheduled());
            long start = System.currentTimeMillis();
            try {
                ProxyPongEntity entity = command.execute().get();
                ProxyPongEntity result = new ProxyPongEntity(direct, entity.getDirect(), System.currentTimeMillis() - start);
                channel.writeAndFlush(result.output());
            } catch (Exception e) {
                channel.writeAndFlush(new ProxyPongEntity(direct, direct, INFINITE).output());
            }
        }
    }
}
