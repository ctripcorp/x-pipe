package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.DefaultSentinelManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Dec 11, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultSentinelManagerTest extends AbstractMetaServerTest{
	
	private DefaultSentinelManager sentinelManager;
	
	private String sentinelMonitorName = "mymaster";
	
	private String allSentinels = "127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004";
	
	private ExecutionLog executionLog;
	
	private RedisMeta redisMaster;
	
	private int port = 6379;
	
	@Mock
	private DcMetaCache dcMetaCache;
	
	@Before
	public void beforeDefaultSentinelManagerTest() throws Exception{
		
		sentinelManager = new DefaultSentinelManager(dcMetaCache, getXpipeNettyClientKeyedObjectPool());
		executionLog = new ExecutionLog();
		redisMaster = new RedisMeta().setIp("127.0.0.1").setPort(port);
		
		when(dcMetaCache.getSentinelMonitorName(getClusterId(), getShardId())).thenReturn(sentinelMonitorName);
		when(dcMetaCache.getSentinel(getClusterId(), getShardId())).thenReturn(new SentinelMeta().setAddress(allSentinels));

	}

	@Test
	public void testRemove(){
		
		sentinelManager.removeSentinel(getClusterId(), getShardId(), executionLog);
		logger.info("{}", executionLog.getLog());
		
	}

	@Test
	public void testAdd(){
		
		sentinelManager.addSentinel(getClusterId(), getShardId(), redisMaster, executionLog);
		logger.info("{}", executionLog.getLog());
	}
	
	
	@Test
	public void testEmpty(){
		
		when(dcMetaCache.getSentinel(getClusterId(), getShardId())).thenReturn(new SentinelMeta().setAddress(""));
		
		sentinelManager.removeSentinel(getClusterId(), getShardId(), executionLog);
		logger.info("{}", executionLog.getLog());
		
	}

}
