package com.ctrip.xpipe.redis.console.service.notifier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.web.client.ResourceAccessException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyInt;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

import java.util.Arrays;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
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
public class ClusterMetaModifiedNotifierTest extends AbstractConsoleTest{
	@Mock
	ConsoleConfig config;
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
		
		when(config.getConsoleNotifyRetryTimes()).thenReturn(retryTimes - 1);
		when(config.getConsoleNotifyRetryInterval()).thenReturn(10);
		when(metaServerConsoleServiceManagerWrapper.get(dcName)).thenReturn(mockedMetaServerConsoleService);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).clusterAdded(clusterName, mockedClusterMeta);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).clusterDeleted(clusterName);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).clusterModified(clusterName, mockedClusterMeta);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).upstreamChange(clusterName, "mockedShardName", "mockedIp", 9999);
		when(clusterMetaService.getClusterMeta(dcName, clusterName)).thenReturn(mockedClusterMeta);
	}
	
	@Test
	public void testNotifyClusterUpdate() {
		notifier.notifyClusterUpdate(dcName, clusterName);
		
		verify(metaServerConsoleServiceManagerWrapper, times(retryTimes)).get(dcName);
		verify(mockedMetaServerConsoleService,times(retryTimes)).clusterModified(clusterName, mockedClusterMeta);
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
		assertEquals(clusterMetaService.getClusterMeta(dcName, clusterName), mockedClusterMeta);
	}
	
	@Test
	public void testNotifyClusterDelete() {
		notifier.notifyClusterDelete(clusterName, Arrays.asList(new DcTbl[]{mockedDcTbl}));
		
		verify(metaServerConsoleServiceManagerWrapper,times(retryTimes)).get(dcName);
		verify(mockedMetaServerConsoleService, times(retryTimes)).clusterDeleted(clusterName);
		verify(mockedMetaServerConsoleService, times(retryTimes)).clusterDeleted(anyString());
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
		assertEquals(clusterMetaService.getClusterMeta(dcName, clusterName), mockedClusterMeta);
	}
	
	@Test
	public void testNotifyUpstreamChanged() {
		notifier.notifyUpstreamChanged(clusterName, "mockedShardName", "mockedIp", 9999, Arrays.asList(new DcTbl[]{mockedDcTbl}));
		
		verify(metaServerConsoleServiceManagerWrapper,times(retryTimes)).get(dcName);
		verify(mockedMetaServerConsoleService, times(retryTimes)).upstreamChange(clusterName, "mockedShardName", "mockedIp", 9999);
		verify(mockedMetaServerConsoleService, times(retryTimes)).upstreamChange(anyString(),anyString(), anyString(), anyInt());
		
		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName),mockedMetaServerConsoleService);
	}
}
