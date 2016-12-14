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
public class TransactionalSlaveOfJobTest extends AbstractMetaServerTest{
	
	
	@Test
	public void testNoneSlavesSuccess() throws Exception{
		
		List<RedisMeta> slaves = new LinkedList<>();
		Command<Void> command = new TransactionalSlaveOfJob(slaves, "localhost", randomPort(), getXpipeNettyClientKeyedObjectPool());
		command.execute().get(1, TimeUnit.SECONDS);
		
	}

}
