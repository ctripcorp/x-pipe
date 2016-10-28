package com.ctrip.xpipe.redis.core.protocal.cmd.manual;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientFactory;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;

/**
 * @author wenchao.meng
 *
 * Oct 28, 2016
 */
public class AbstractCommandTest extends AbstractRedisTest{

	protected FixedObjectPool<NettyClient> createClientPool(String host, int port) throws Exception {

		NettyClientFactory nettyClientFactory = new NettyClientFactory(new InetSocketAddress(host, port));
		NettyClient nettyClient = nettyClientFactory.makeObject().getObject();
		FixedObjectPool<NettyClient> clientPool = new FixedObjectPool<NettyClient>(nettyClient);
		return clientPool;
	}

}
