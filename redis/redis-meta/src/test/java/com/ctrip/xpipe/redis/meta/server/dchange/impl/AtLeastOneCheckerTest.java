package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.AtLeastOneChecker;

/**
 * @author wenchao.meng
 *
 * Dec 1, 2016
 */
public class AtLeastOneCheckerTest extends AbstractMetaServerTest{
	
	@Test
	public void testCheckerTimeout() throws Exception{

		PingCommand.DEFAULT_PINT_TIME_OUT_MILLI = 10;

		Server server = startServer((String) null);
		List<RedisMeta> redises = new LinkedList<>();
		redises.add(new RedisMeta().setIp("localhost").setPort(server.getPort()));
		Assert.assertFalse(new AtLeastOneChecker(redises, getXpipeNettyClientKeyedObjectPool(), scheduled).check());
		
	}

}
