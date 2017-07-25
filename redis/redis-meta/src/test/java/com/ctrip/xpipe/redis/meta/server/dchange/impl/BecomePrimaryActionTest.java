package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomePrimaryAction;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Dec 12, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class BecomePrimaryActionTest extends AbstractMetaServerTest{
	
	@Mock
	private DcMetaCache dcMetaCache;

	@Mock
	private CurrentMetaManager currentMetaManager;
	
	@Mock
	private SentinelManager sentinelManager;
	
	@Mock
	private NewMasterChooser newMasterChooser;
	
	private String newPrimaryDc = "jq";

	@SuppressWarnings("unchecked")
	@Before
	public void beforeBecomePrimaryActionTest() throws Exception{
				
		List<RedisMeta> redises = new LinkedList<>();
		RedisMeta redis1 = new RedisMeta().setIp("localhost").setPort(6379); 
		RedisMeta redis2 = new RedisMeta().setIp("localhost").setPort(6479);
		
		redises.add(redis1);
		redises.add(redis2);

		when(dcMetaCache.getShardRedises(getClusterId(), getShardId())).thenReturn(redises);
		
		when(newMasterChooser.choose(anyList())).thenReturn(redis1);
	}

	@Test
	public void test() throws Exception{
		
		BecomePrimaryAction becomePrimaryAction = new BecomePrimaryAction(dcMetaCache, currentMetaManager, sentinelManager, getXpipeNettyClientKeyedObjectPool(), newMasterChooser, scheduled, executors);
		
		PrimaryDcChangeMessage message = becomePrimaryAction.changePrimaryDc(getClusterId(), getShardId(), newPrimaryDc);
		
		logger.info("{}", message);
	}

}
