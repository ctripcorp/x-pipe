package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigRewrite;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand.ConfigSetMinSlavesToWrite;
import com.ctrip.xpipe.redis.core.protocal.cmd.transaction.TransactionalCommand;
import com.ctrip.xpipe.redis.meta.server.dcchange.RedisReadonly;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
@Deprecated
public class MinSlavesRedisReadOnly extends AbstractRedisReadOnly implements RedisReadonly{
	
	public static int READ_ONLY_NUMBER = Integer.MAX_VALUE;
	
	public static int WRITABLE_NUMBER = 0;
	
	public MinSlavesRedisReadOnly(String ip, int port, XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled) {
		super(ip, port, keyedObjectPool, scheduled);
	}

	@Override
	protected Command<?> createReadOnlyCommand() {
		return createTransactionalCommand(READ_ONLY_NUMBER);
	}


	@Override
	protected Command<?> createWritableCommand() {
		return createTransactionalCommand(WRITABLE_NUMBER);
	}

	private Command<?> createTransactionalCommand(int number){
		
		SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(ip, port));
		ConfigSetMinSlavesToWrite configSetMinSlavesToWrite = new ConfigSetMinSlavesToWrite(null, number, scheduled);
		
		return new TransactionalCommand(clientPool, scheduled, configSetMinSlavesToWrite, new ConfigRewrite(null, scheduled));
	}


}
