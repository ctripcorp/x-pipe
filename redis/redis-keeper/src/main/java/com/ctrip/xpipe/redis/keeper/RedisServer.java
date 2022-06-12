package com.ctrip.xpipe.redis.keeper;


import com.ctrip.xpipe.api.server.Server;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:24:05
 */
public interface RedisServer extends Server, Infoable{

    RedisClient clientConnected(Channel channel);

    void clientDisconnected(Channel channel);

    void processCommandSequentially(Runnable runnable);

}
