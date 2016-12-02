package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand.ConfigSetMinSlavesToWrite;
import com.ctrip.xpipe.redis.meta.server.dcchange.RedisReadonly;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public class MinSlavesRedisReadOnly implements RedisReadonly{
	
	private Logger logger = LoggerFactory.getLogger(MinSlavesRedisReadOnly.class);

	private int READ_ONLY_NUMBER = Integer.MAX_VALUE;
	
	private int WRITABLE_NUMBER = 0;
	
	private String ip;
	
	private int port;
	
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;
	
	public MinSlavesRedisReadOnly(String ip, int port, XpipeNettyClientKeyedObjectPool keyedObjectPool) {
		this.ip = ip;
		this.port = port;
		this.keyedObjectPool = keyedObjectPool;
	}

	@Override
	public void makeReadOnly() throws Exception {
		
		ConfigSetMinSlavesToWrite command = new ConfigSetMinSlavesToWrite(keyedObjectPool.getKeyPool(new InetSocketAddress(ip, port)), READ_ONLY_NUMBER);
		Boolean result = command.execute().get();
		logger.info("[makeReadOnly]{}:{}, {}", ip, port, result);
		
	}

	@Override
	public void makeWritable() throws Exception {
		
		ConfigSetMinSlavesToWrite command = new ConfigSetMinSlavesToWrite(keyedObjectPool.getKeyPool(new InetSocketAddress(ip, port)), WRITABLE_NUMBER);
		Boolean result = command.execute().get();
		logger.info("[makeWritable]{}:{}, {}", ip, port, result);
	}

}
