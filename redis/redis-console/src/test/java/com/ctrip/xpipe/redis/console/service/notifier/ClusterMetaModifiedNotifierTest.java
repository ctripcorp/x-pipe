package com.ctrip.xpipe.redis.console.service.notifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyInt;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;

/**
 * @author shyin
 *
 * Sep 14, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterMetaModifiedNotifierTest {
	@Mock
	ClusterMetaService clusterMetaService;
	@Mock
	MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;
	@InjectMocks
	DefaultClusterMetaModifiedNotifier notifier;
	
	@Mock
	MetaServerConsoleService mockedMetaServerConsoleService;
	
	private String dcName;
	private DcTbl mockedDcTbl;
	private String clusterName;
	private ClusterMeta mockedClusterMeta;
	
	
	@Before
	public void initMockData() {
		dcName = "mockedDc";
		mockedDcTbl = new DcTbl().setDcName(dcName);
		clusterName = "mockedClusterName";
		mockedClusterMeta = new ClusterMeta().setId(clusterName).setActiveDc(dcName);
		
		when(metaServerConsoleServiceManagerWrapper.get(dcName)).thenReturn(mockedMetaServerConsoleService);
		when(clusterMetaService.getClusterMeta(dcName, clusterName)).thenReturn(mockedClusterMeta);
	}
	
	@Test
	public void testNotifyClusterUpdate() {
		notifier.notifyClusterUpdate(dcName, clusterName);
		
		verify(metaServerConsoleServiceManagerWrapper, times(1)).get(dcName);
		verify(mockedMetaServerConsoleService,times(1)).clusterModified(clusterName, mockedClusterMeta);
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
		assertEquals(clusterMetaService.getClusterMeta(dcName, clusterName), mockedClusterMeta);
	}
	
	@Test
	public void testNotifyClusterDelete() {
		notifier.notifyClusterDelete(clusterName, Arrays.asList(new DcTbl[]{mockedDcTbl}));
		
		verify(metaServerConsoleServiceManagerWrapper,times(1)).get(dcName);
		verify(mockedMetaServerConsoleService, times(1)).clusterDeleted(clusterName);
		verify(mockedMetaServerConsoleService, times(1)).clusterDeleted(anyString());
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
		assertEquals(clusterMetaService.getClusterMeta(dcName, clusterName), mockedClusterMeta);
	}
	
	@Test
	public void testNotifyUpstreamChanged() {
		notifier.notifyUpstreamChanged(clusterName, "mockedShardName", "mockedIp", 9999, Arrays.asList(new DcTbl[]{mockedDcTbl}));
		
		verify(metaServerConsoleServiceManagerWrapper,times(1)).get(dcName);
		verify(mockedMetaServerConsoleService, times(1)).upstreamChange(clusterName, "mockedShardName", "mockedIp", 9999);
		verify(mockedMetaServerConsoleService, times(1)).upstreamChange(anyString(),anyString(), anyString(), anyInt());
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
	}
}
