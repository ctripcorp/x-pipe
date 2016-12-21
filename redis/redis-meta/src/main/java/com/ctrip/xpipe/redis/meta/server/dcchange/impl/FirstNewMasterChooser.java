package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;

/**
 * choose first configed alive slave in console
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class FirstNewMasterChooser extends AbstractNewMasterChooser{
	
	
	public FirstNewMasterChooser(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled) {
		super(keyedObjectPool, scheduled);
	}

	@Override
	protected RedisMeta doChoose(List<RedisMeta> redises) {
		
		for(RedisMeta redisMeta : redises){
			if(isAlive(redisMeta)){
				return redisMeta;
			}
		}
		return null;
	}

	private boolean isAlive(RedisMeta redisMeta) {
		
		SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort())); 
		try {
			new PingCommand(clientPool, scheduled).execute().get();
			return true;
		} catch (InterruptedException | ExecutionException e) {
			logger.info("[isAlive]" + redisMeta, e);
		}
		return false;
	}
}
