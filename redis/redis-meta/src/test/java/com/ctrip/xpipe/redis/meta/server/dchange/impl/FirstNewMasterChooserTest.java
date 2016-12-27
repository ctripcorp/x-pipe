package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.FirstNewMasterChooser;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class FirstNewMasterChooserTest extends AbstractMetaServerTest{

	private List<RedisMeta> redises;
	
	private FirstNewMasterChooser firstNewMasterChooser;

	@Before
	public void beforeFirstNewMasterChooserTest() throws Exception{
		
		redises = new LinkedList<>();
		int port1 = randomPort();
		redises.add(new RedisMeta().setIp("localhost").setPort(port1));
		redises.add(new RedisMeta().setIp("localhost").setPort(randomPort(Arrays.asList(port1))));
		
		firstNewMasterChooser = new FirstNewMasterChooser(getXpipeNettyClientKeyedObjectPool(), scheduled);
	}
	
	@Test
	public void testChooseFirstAlive() throws Exception{
		
		Assert.assertNull(firstNewMasterChooser.choose(redises));
		startFakeRedisServer(redises.get(1).getPort());

		Assert.assertEquals(redises.get(1), firstNewMasterChooser.choose(redises));

		startFakeRedisServer(redises.get(0).getPort());

		Assert.assertEquals(redises.get(0), firstNewMasterChooser.choose(redises));
	}
	
	@Test
	public void testChooseExistingMaster() throws Exception{

		SlaveRole role = new SlaveRole(SERVER_ROLE.MASTER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0L);
		RedisMeta chosen = redises.get(1);
		startServer(chosen.getPort(), ByteBufUtils.readToString(role.format()));
		
		Assert.assertEquals(chosen, firstNewMasterChooser.choose(redises));
	}
	
	//run with real redis
//	@Test
	public void testRedis(){
		
		redises.clear();
		redises.add(new RedisMeta().setIp("localhost").setPort(6379));
		redises.add(new RedisMeta().setIp("localhost").setPort(6479));
		logger.info("{}", firstNewMasterChooser.choose(redises));
	}
}
