package com.ctrip.xpipe.redis.integratedtest.health;

import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Jul 29, 2018
 */
public class ProxableNettyClientFactory extends NettyKeyedPoolClientFactory {

    private int eventLoopThreads;
    private NioEventLoopGroup eventLoopGroup;
    private Bootstrap b = new Bootstrap();
    private int connectTimeoutMilli = 5000;
    private static Logger logger = LoggerFactory.getLogger(NettyKeyedPoolClientFactory.class);

    private List<ProxyEndpoint> tcpProxies;

    private ProxyEndpointSelector selector;

    public ProxableNettyClientFactory(List<ProxyEndpoint> tcpProxies) {
        this(DEFAULT_KEYED_POOLED_CLIENT_FACTORY_EVNET_LOOP_THREAD);
        this.tcpProxies = tcpProxies;
        ProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()->2);
        this.selector = new DefaultProxyEndpointSelector(tcpProxies, endpointManager);
        selector.setSelectStrategy(new SelectNTimes(selector, SelectNTimes.INFINITE));
        selector.setNextHopAlgorithm(new NaiveNextHopAlgorithm());
    }

    public ProxableNettyClientFactory(int eventLoopThreads) {
        this.eventLoopThreads = eventLoopThreads;

    }

    @Override
    public PooledObject<NettyClient> makeObject(InetSocketAddress key) throws Exception {

        ChannelFuture f = b.connect(selector.nextHop().getSocketAddress());
        f.get(connectTimeoutMilli, TimeUnit.MILLISECONDS);
        Channel channel = f.channel();
        logger.debug("[makeObject]{}", channel);
        String protocol = String.format("PROXY ROUTE TCP://%s:%d", key.getHostName(), key.getPort());
        channel.writeAndFlush(new RequestStringParser(protocol).format());
        NettyClient nettyClient = new DefaultNettyClient(channel);
        channel.attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
        return new DefaultPooledObject<NettyClient>(nettyClient);
    }

    @Override
    public void destroyObject(InetSocketAddress key, PooledObject<NettyClient> p) throws Exception {

        logger.info("[destroyObject]{}, {}", key, p.getObject());
        p.getObject().channel().close();

    }

    @Override
    public boolean validateObject(InetSocketAddress key, PooledObject<NettyClient> p) {
        return p.getObject().channel().isActive();
    }

    @Override
    public void activateObject(InetSocketAddress key, PooledObject<NettyClient> p) throws Exception {

    }

    @Override
    public void passivateObject(InetSocketAddress key, PooledObject<NettyClient> p) throws Exception {

    }


    @Override
    protected void doStop() {
        eventLoopGroup.shutdownGracefully();
    }
}
