package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.exception.SimpleErrorMessage;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import com.ctrip.xpipe.redis.meta.server.dcchange.HealthChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 1, 2016
 */
public class AtLeastOneChecker implements HealthChecker{
	
	private static Logger logger = LoggerFactory.getLogger(AtLeastOneChecker.class); 
	
	private List<RedisMeta> redises;
	
	private XpipeNettyClientKeyedObjectPool pool;
	
	private ScheduledExecutorService scheduled;
	
	public AtLeastOneChecker(List<RedisMeta> list, XpipeNettyClientKeyedObjectPool pool, ScheduledExecutorService scheduled) {
		this.redises = list;
		this.pool = pool;
		this.scheduled = scheduled;
	}

	@Override
	public SimpleErrorMessage check() {

		StringBuilder sb = new StringBuilder();
		for(Redis  redis : redises){
			try {
				new PingCommand(pool.getKeyPool(new InetSocketAddress(redis.getIp(), redis.getPort())), scheduled).execute().get();
				return SimpleErrorMessage.success();
			} catch (InterruptedException | ExecutionException e) {
				logger.info("[check]", e);
				sb.append(String.format("%s: %s\r\n", redis.desc(), ExceptionUtils.getRootCause(e).getMessage()));
			}
		}
		return SimpleErrorMessage.fail(sb.toString());
	}
}
