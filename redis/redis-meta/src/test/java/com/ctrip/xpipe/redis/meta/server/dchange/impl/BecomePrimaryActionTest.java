package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.dcchange.OffsetWaiter;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomePrimaryAction;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.when;

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

	@Mock
	private OffsetWaiter offsetWaiter;
	
	private String newPrimaryDc = "jq";

	private Server master;

	private Server slave;

	@SuppressWarnings("unchecked")
	@Before
	public void beforeBecomePrimaryActionTest() throws Exception{
		master = startServer("+OK\r\n");
		slave = startServer("+OK\r\n");
				
		List<RedisMeta> redises = new LinkedList<>();
		RedisMeta redis1 = new RedisMeta().setIp("127.0.0.1").setPort(master.getPort());
		RedisMeta redis2 = new RedisMeta().setIp("127.0.0.1").setPort(slave.getPort());
		
		redises.add(redis1);
		redises.add(redis2);

		when(dcMetaCache.getShardRedises(getClusterId(), getShardId())).thenReturn(redises);
		
		when(newMasterChooser.choose(anyList())).thenReturn(redis1);
	}

	@After
	public void afterBecomePrimaryActionTest() throws Exception {
		if (null != master) {
			master.stop();
			master = null;
		}
		if (null != slave) {
			slave.stop();
			slave = null;
		}
	}

	@Test
	public void testMasterSlaveAllRight() throws Exception {
		BecomePrimaryAction becomePrimaryAction = new BecomePrimaryAction("cluster", "shard", dcMetaCache,
				currentMetaManager, sentinelManager, offsetWaiter, new ExecutionLog(getTestName()), getXpipeNettyClientKeyedObjectPool(), newMasterChooser, scheduled, executors);
		PrimaryDcChangeMessage message = becomePrimaryAction.changePrimaryDc(getClusterId(), getShardId(), newPrimaryDc, new MasterInfo());
		Assert.assertEquals(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, message.getErrorType());
	}

	@Test
	public void testSlaveDown() throws Exception {
		slave.stop();
		slave = null;

		BecomePrimaryAction becomePrimaryAction = new BecomePrimaryAction("cluster", "shard", dcMetaCache,
				currentMetaManager, sentinelManager, offsetWaiter, new ExecutionLog(getTestName()), getXpipeNettyClientKeyedObjectPool(), newMasterChooser, scheduled, executors);
		PrimaryDcChangeMessage message = becomePrimaryAction.changePrimaryDc(getClusterId(), getShardId(), newPrimaryDc, new MasterInfo());
		Assert.assertEquals(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.SUCCESS, message.getErrorType());
	}

	@Test
	public void testMasterDown() throws Exception {
		master.stop();
		master = null;

		BecomePrimaryAction becomePrimaryAction = new BecomePrimaryAction("cluster", "shard", dcMetaCache,
				currentMetaManager, sentinelManager, offsetWaiter, new ExecutionLog(getTestName()), getXpipeNettyClientKeyedObjectPool(), newMasterChooser, scheduled, executors);
		PrimaryDcChangeMessage message = becomePrimaryAction.changePrimaryDc(getClusterId(), getShardId(), newPrimaryDc, new MasterInfo());
		Assert.assertEquals(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.FAIL, message.getErrorType());
	}

}
