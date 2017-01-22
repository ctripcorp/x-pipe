package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigRewrite;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand.ConfigSetMinSlavesToWrite;
import com.ctrip.xpipe.redis.core.protocal.cmd.transaction.TransactionalCommand;
import com.ctrip.xpipe.redis.meta.server.dcchange.RedisReadonly;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public class MinSlavesRedisReadOnly implements RedisReadonly{
	
	private Logger logger = LoggerFactory.getLogger(MinSlavesRedisReadOnly.class);

	public static int READ_ONLY_NUMBER = Integer.MAX_VALUE;
	
	public static int WRITABLE_NUMBER = 0;
	
	private String ip;
	
	private int port;
	
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;
	
	private ScheduledExecutorService scheduled;
	
	public MinSlavesRedisReadOnly(String ip, int port, XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled) {
		this.ip = ip;
		this.port = port;
		this.keyedObjectPool = keyedObjectPool;
		this.scheduled = scheduled;
	}

	@Override
	public void makeReadOnly() throws Exception {
		
		Command<Object[]> command = createTransactionalCommand(READ_ONLY_NUMBER);
		Object []result = command.execute().get();
		
		logger.info("[makeReadOnly]{}:{}, {}", ip, port, (Object)result);
		
	}
	
	private Command<Object[]> createTransactionalCommand(int number){
		
		SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(ip, port));
		ConfigSetMinSlavesToWrite configSetMinSlavesToWrite = new ConfigSetMinSlavesToWrite(null, number, scheduled);
		
		return new TransactionalCommand(clientPool, scheduled, configSetMinSlavesToWrite, new ConfigRewrite(null, scheduled));
	}

	@Override
	public void makeWritable() throws Exception {
		
		Command<Object[]> command = createTransactionalCommand(WRITABLE_NUMBER);
		Object []result = command.execute().get();
		logger.info("[makeWritable]{}:{}, {}", ip, port, (Object)result);
	}

}
