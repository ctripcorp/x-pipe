package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
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
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.client.ResourceAccessException;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author shyin
 *
 *         Sep 14, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterMetaModifiedNotifierTest extends AbstractConsoleTest {
	@Mock
	ConsoleConfig config;
	@Mock
	ClusterMetaService clusterMetaService;
	@Mock
	MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;
	@Spy
	@InjectMocks
	DefaultClusterMetaModifiedNotifier notifier;

	@Mock
	MetaServerConsoleService mockedMetaServerConsoleService;

	private String dcName;
	private DcTbl mockedDcTbl;
	private String clusterName;
	private ClusterMeta mockedClusterMeta;
	private int retryTimes = 5;

	@Before
	public void initMockData() {
		dcName = "mockedDc";
		mockedDcTbl = new DcTbl().setDcName(dcName);
		clusterName = "mockedClusterName";
		mockedClusterMeta = new ClusterMeta().setId(clusterName).setActiveDc(dcName);

		when(config.getConsoleNotifyRetryTimes()).thenReturn(retryTimes - 1);
		when(config.getConsoleNotifyRetryInterval()).thenReturn(10);
		when(config.getConsoleNotifyThreads()).thenReturn(20);

		notifier.postConstruct();

		when(metaServerConsoleServiceManagerWrapper.get(dcName)).thenReturn(mockedMetaServerConsoleService);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).clusterDeleted(clusterName);
		doThrow(new ResourceAccessException("test")).when(mockedMetaServerConsoleService).clusterModified(clusterName,
				mockedClusterMeta);
		when(clusterMetaService.getClusterMeta(dcName, clusterName)).thenReturn(mockedClusterMeta);
	}

	@Test
	public void testNotifyClusterUpdate() throws InterruptedException {
		notifier.notifyClusterUpdate(clusterName, Collections.singletonList(dcName));
		TimeUnit.SECONDS.sleep(1);
		verify(metaServerConsoleServiceManagerWrapper, times(retryTimes)).get(dcName);
		verify(mockedMetaServerConsoleService, times(retryTimes)).clusterModified(clusterName, mockedClusterMeta);
		verify(notifier, times(1)).submitNotifyTask(any(MetaNotifyTask.class));

		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName), mockedMetaServerConsoleService);
		assertEquals(clusterMetaService.getClusterMeta(dcName, clusterName), mockedClusterMeta);
	}

	@Test
	public void testNotifyClusterDelete() throws InterruptedException {
		notifier.notifyClusterDelete(clusterName, Arrays.asList(new DcTbl[] { mockedDcTbl }));
		TimeUnit.SECONDS.sleep(1);
		verify(metaServerConsoleServiceManagerWrapper, times(retryTimes)).get(dcName);
		verify(mockedMetaServerConsoleService, times(retryTimes)).clusterDeleted(clusterName);
		verify(mockedMetaServerConsoleService, times(retryTimes)).clusterDeleted(anyString());
		verify(notifier, times(1)).submitNotifyTask(any(MetaNotifyTask.class));

		assertEquals(metaServerConsoleServiceManagerWrapper.get(dcName), mockedMetaServerConsoleService);
		assertEquals(clusterMetaService.getClusterMeta(dcName, clusterName), mockedClusterMeta);
	}

}
