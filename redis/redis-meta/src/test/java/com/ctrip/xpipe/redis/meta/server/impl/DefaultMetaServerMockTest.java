package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaServerMockTest {

	@InjectMocks
	private DefaultMetaServer metaServer;

	@Mock
	private CurrentMetaManager currentMetaManager;

	@Mock
	private DcMetaCache dcMetaCache;

	@Mock
	private ForwardInfo forwardInfo;

	@Before
	public void setUp() {
		when(dcMetaCache.clusterShardId2DbId(anyString(), anyString())).thenReturn(new Pair<>(1L, 1L));
		when(dcMetaCache.getPrimaryDc(anyLong(), anyLong())).thenReturn("primaryDc");
		when(dcMetaCache.isCurrentShardParentCluster(anyLong(), anyLong())).thenReturn(true);
	}



	@Test
	public void testUpdateUpstreamCurrentDcIsPrimary() {
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(true);
		metaServer.updateUpstream("dcName", "clusterId", "shardId", "ip", 6379, forwardInfo);
		verify(currentMetaManager, never()).setKeeperMaster(anyLong(), anyLong(), anyString(), anyInt());
	}

	@Test
	public void testUpdateUpstreamDcNotPrimary() {
		when(dcMetaCache.isCurrentDcPrimary(anyLong(), anyLong())).thenReturn(false);
		when(dcMetaCache.getPrimaryDc(anyLong(), anyLong())).thenReturn("anotherPrimaryDc");
		metaServer.updateUpstream("dcName", "clusterId", "shardId", "ip", 6379, forwardInfo);
		verify(currentMetaManager, never()).setKeeperMaster(anyLong(), anyLong(), anyString(), anyInt());
	}
}
