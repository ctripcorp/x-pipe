package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.server.AbstractServer;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:24:55
 */
public abstract class AbstractRedisServer extends AbstractServer implements RedisServer{

    protected final Map<Channel, RedisClient> redisClients = new ConcurrentHashMap<Channel, RedisClient>();

}
