package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 *         Aug 31, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultCurrentMetaManagerTest extends AbstractMetaServerContextTest {

	private DefaultCurrentMetaManager currentMetaServerMetaManager;

	@Mock
	private SlotManager slotManager;
	
	@Mock
	private DcMetaCache dcMetaCache;

	@Before
	public void beforeDefaultCurrentMetaServerMetaManagerTest() {

		currentMetaServerMetaManager = getBean(DefaultCurrentMetaManager.class);
		currentMetaServerMetaManager.setSlotManager(slotManager);
		currentMetaServerMetaManager.setDcMetaCache(dcMetaCache);
	}

	@Test
	public void testCheckAddOrRemoveSlots(){
		
		Set<Integer> newSlots = new HashSet<>();
		for(int i=0;i<10;i++){
			newSlots.add(i);
		}
		
		Assert.assertEquals(0, currentMetaServerMetaManager.getCurrentSlots().size());
		
		when(slotManager.getSlotsByServerId(anyInt(), eq(false))).thenReturn(newSlots);

		currentMetaServerMetaManager.checkAddOrRemoveSlots();
		
		logger.info("[testCheckAddOrRemoveSlots]{}", currentMetaServerMetaManager.getCurrentSlots());
		
		Assert.assertEquals(newSlots, currentMetaServerMetaManager.getCurrentSlots());

		for(int i=0;i<5;i++){
			newSlots.remove(i);
		}
		for(int i=10;i<20;i++){
			newSlots.add(i);
		}
		
		currentMetaServerMetaManager.checkAddOrRemoveSlots();
		Assert.assertEquals(newSlots, currentMetaServerMetaManager.getCurrentSlots());
	}

	@Test
	public void testAddOrRemove() {

		Set<Integer> future = new HashSet<>();
		future.add(1);
		future.add(2);
		future.add(3);

		Set<Integer> current = new HashSet<>();
		current.add(1);
		current.add(2);
		current.add(4);

		Pair<Set<Integer>, Set<Integer>> result = currentMetaServerMetaManager.getAddAndRemove(future, current);

		Assert.assertEquals(3, future.size());
		Assert.assertEquals(3, current.size());

		Assert.assertEquals(1, result.getKey().size());
		Assert.assertEquals(3, result.getKey().toArray()[0]);

		Assert.assertEquals(1, result.getValue().size());
		Assert.assertEquals(4, result.getValue().toArray()[0]);
	}

	@Override
	protected boolean isStartZk() {
		return false;
	}

	@Test
	public void testRouteChange() {
		currentMetaServerMetaManager = spy(currentMetaServerMetaManager);

		currentMetaServerMetaManager.update(new DcRouteMetaComparator(null, null), null);
		verify(currentMetaServerMetaManager, never()).dcMetaChange(any());

		verify(currentMetaServerMetaManager, times(1)).routeChanges();
	}

	@Test
	public void testRefreshKeeperMaster() {
		currentMetaServerMetaManager = spy(new DefaultCurrentMetaManager());
		currentMetaServerMetaManager.setDcMetaCache(dcMetaCache);
		String clusterId = getClusterId();

		when(currentMetaServerMetaManager.allClusters()).thenReturn(Sets.newHashSet(clusterId));
		Assert.assertFalse(currentMetaServerMetaManager.allClusters().isEmpty());

		doReturn(Pair.from("127.0.0.1", randomPort())).when(currentMetaServerMetaManager).getKeeperMaster(anyString(), anyString());

		when(dcMetaCache.randomRoute(clusterId)).thenReturn(new RouteMeta(1000).setTag(Route.TAG_META));
		when(dcMetaCache.getClusterMeta(clusterId)).thenReturn(getCluster(getDcs()[0], clusterId));

		int times = getCluster(getDcs()[0], clusterId).getShards().size();
		MetaServerStateChangeHandler handler = mock(MetaServerStateChangeHandler.class);
		currentMetaServerMetaManager.addMetaServerStateChangeHandler(handler);
		currentMetaServerMetaManager.routeChanges();

		verify(handler, times(times)).keeperMasterChanged(eq(clusterId), anyString(), any());
	}

}
