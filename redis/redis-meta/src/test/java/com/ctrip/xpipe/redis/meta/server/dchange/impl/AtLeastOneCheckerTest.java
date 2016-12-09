package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import java.util.LinkedList;
import java.util.List;

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
	public void test() throws Exception{
		
		List<RedisMeta> redises = new LinkedList<>();
		
		redises.add(new RedisMeta().setIp("localhost").setPort(6379));
		redises.add(new RedisMeta().setIp("localhost").setPort(6479));
		
		logger.info("{}", new AtLeastOneChecker(redises, getXpipeNettyClientKeyedObjectPool()).check());
		
	}

}
