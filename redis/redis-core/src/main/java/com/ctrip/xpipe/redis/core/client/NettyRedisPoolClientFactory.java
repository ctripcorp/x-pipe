package com.ctrip.xpipe.redis.core.client;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import io.netty.channel.ChannelFuture;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class NettyRedisPoolClientFactory extends NettyKeyedPoolClientFactory {

    private String clientName;

    private AsyncConnectionCondition asyncConnectionCondition;

    public NettyRedisPoolClientFactory(int eventLoopThreads, String clientName, AsyncConnectionCondition asyncConnectionCondition) {
        super(eventLoopThreads);
        this.clientName = clientName;
        this.asyncConnectionCondition = asyncConnectionCondition;
    }

    @Override
    public PooledObject<NettyClient> makeObject(Endpoint key) throws Exception {
        ChannelFuture f = b.connect(key.getHost(), key.getPort());
        NettyClient nettyClient = new RedisAsyncNettyClient(f, key, clientName, asyncConnectionCondition);
        f.channel().attr(NettyClientHandler.KEY_CLIENT).set(nettyClient);
        return new DefaultPooledObject<NettyClient>(nettyClient);
    }

}
