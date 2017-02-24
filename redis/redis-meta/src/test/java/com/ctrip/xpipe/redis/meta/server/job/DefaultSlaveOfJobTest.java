package com.ctrip.xpipe.redis.meta.server.job;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;

/**
 * @author wenchao.meng
 *
 * Dec 14, 2016
 */
public class DefaultSlaveOfJobTest extends AbstractMetaServerTest{
	
	@Test
	public void testNoneSlavesSuccess() throws Exception{
		
		List<RedisMeta> slaves = new LinkedList<>();
		Command<Void> command = new DefaultSlaveOfJob(slaves, "localhost", randomPort(), getXpipeNettyClientKeyedObjectPool(), scheduled);
		command.execute().get(1, TimeUnit.SECONDS);
	}
	
	
	@Test
	public void testSlavesSuccess() throws Exception{
		
		List<RedisMeta> slaves = new LinkedList<>();
		slaves.add(new RedisMeta().setIp("localhost").setPort(6479));
		slaves.add(new RedisMeta().setIp("localhost").setPort(6579));
		
		Command<Void> command = new DefaultSlaveOfJob(slaves, "localhost", randomPort(), getXpipeNettyClientKeyedObjectPool(), scheduled);
		command.execute().get(1, TimeUnit.SECONDS);
		
	}


}
