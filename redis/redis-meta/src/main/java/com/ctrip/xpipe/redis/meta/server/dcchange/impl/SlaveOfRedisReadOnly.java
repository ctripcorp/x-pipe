package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultSlaveOfCommand;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Feb 24, 2017
 */
public class SlaveOfRedisReadOnly extends AbstractRedisReadOnly{
	
	private static final String slaveHost = "127.0.0.1";
	private static final int    slavePort = 0;
		
	public SlaveOfRedisReadOnly(String ip, int port, XpipeNettyClientKeyedObjectPool keyedObjectPool,
			ScheduledExecutorService scheduled) {
		super(ip, port, keyedObjectPool, scheduled);
	}

	@Override
	protected Command<?> createReadOnlyCommand() {

		SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(ip, port));

		return new DefaultSlaveOfCommand(clientPool, slaveHost, slavePort, scheduled);
	}

	@Override
	protected Command<?> createWritableCommand() {
		
		SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(ip, port));
		return new DefaultSlaveOfCommand(clientPool, scheduled);
	}

}
