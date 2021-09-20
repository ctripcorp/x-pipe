package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomeBackupAction;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Dec 12, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class BecomeBackupActionTest extends AbstractMetaServerTest{
	
	@Mock
	private DcMetaCache dcMetaCache;

	@Mock
	private CurrentMetaManager currentMetaManager;
	
	@Mock
	private MultiDcService multiDcService;

	@Mock
	private SentinelManager sentinelManager;
	
	private String newPrimaryDc = "jq";

	@Before
	public void beforeBecomePrimaryActionTest() throws Exception{
		
		List<RedisMeta> redises = new LinkedList<>();
		redises.add(new RedisMeta().setIp("localhost").setPort(6379));
		redises.add(new RedisMeta().setIp("localhost").setPort(6479));

		when(dcMetaCache.getShardRedises(getClusterId(), getShardId())).thenReturn(redises);
	}

	@Test
	public void test() throws Exception{
		
		BecomeBackupAction becomeBackupAction = new BecomeBackupAction(dcMetaCache, currentMetaManager, sentinelManager, new ExecutionLog(currentTestName()), getXpipeNettyClientKeyedObjectPool(), multiDcService, scheduled, executors);
		
		PrimaryDcChangeMessage message = becomeBackupAction.changePrimaryDc(getClusterId(), getShardId(), newPrimaryDc, new MasterInfo());
		
		logger.info("{}", message);
	}
}
