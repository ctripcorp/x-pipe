package com.ctrip.xpipe.redis.console.service.notifier;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.web.client.ResourceAccessException;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author shyin
 *
 * Sep 14, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterMetaModifiedNotifierTest extends AbstractConsoleTest{
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

	private int retryTimes = 10;
	
	@Before
	public void initMockData() {
		dcName = "mockedDc";
		mockedDcTbl = new DcTbl().setDcName(dcName);
		clusterName = "mockedClusterName";
		mockedClusterMeta = new ClusterMeta().setId(clusterName).setActiveDc(dcName);
		
		when(metaServerConsoleServiceManagerWrapper.get(dcName)).thenReturn(mockedMetaServerConsoleService);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).clusterAdded(clusterName, mockedClusterMeta);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).clusterDeleted(clusterName);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).clusterModified(clusterName, mockedClusterMeta);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).upstreamChange(anyString(), anyString(), anyString(), anyInt());
		when(clusterMetaService.getClusterMeta(dcName, clusterName)).thenReturn(mockedClusterMeta);
	}
	
	@Test
	public void testNotifyClusterUpdate() {
		notifier.notifyClusterUpdate(dcName, clusterName);
		
		verify(metaServerConsoleServiceManagerWrapper, times(retryTimes + 1)).get(dcName);
		verify(mockedMetaServerConsoleService,times(retryTimes + 1)).clusterModified(clusterName, mockedClusterMeta);
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
		assertEquals(clusterMetaService.getClusterMeta(dcName, clusterName), mockedClusterMeta);
	}
	
	@Test
	public void testNotifyClusterDelete() {
		notifier.notifyClusterDelete(clusterName, Arrays.asList(new DcTbl[]{mockedDcTbl}));
		
		verify(metaServerConsoleServiceManagerWrapper,times(retryTimes + 1)).get(dcName);
		verify(mockedMetaServerConsoleService, times(retryTimes + 1)).clusterDeleted(clusterName);
		verify(mockedMetaServerConsoleService, times(retryTimes + 1)).clusterDeleted(anyString());
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
		assertEquals(clusterMetaService.getClusterMeta(dcName, clusterName), mockedClusterMeta);
	}
	
	@Test
	public void testNotifyUpstreamChanged() {
		notifier.notifyUpstreamChanged(clusterName, "mockedShardName", "mockedIp", 9999, Arrays.asList(new DcTbl[]{mockedDcTbl}));
		
		verify(metaServerConsoleServiceManagerWrapper,times(retryTimes + 1)).get(dcName);
		verify(mockedMetaServerConsoleService, times(retryTimes + 1)).upstreamChange(clusterName, "mockedShardName", "mockedIp", 9999);
		verify(mockedMetaServerConsoleService, times(retryTimes + 1)).upstreamChange(anyString(),anyString(), anyString(), anyInt());
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
	}
}
