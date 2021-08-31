package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.RouteMetaComparator;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMeta;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
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
	private CurrentClusterServer currentClusterServer; 

	@Mock
	private SlotManager slotManager;
	
	@Mock
	private DcMetaCache dcMetaCache;

	@Mock
	private MetaServerStateChangeHandler handler;

	@Mock
	private CurrentMeta currentMeta;
	
	@Mock
	private Observer observer; 

	private String upstreamDc = "upstream-dc";

	@Before
	public void beforeDefaultCurrentMetaServerMetaManagerTest() {

		currentMetaServerMetaManager = getBean(DefaultCurrentMetaManager.class);
		currentMetaServerMetaManager.setSlotManager(slotManager);
		currentMetaServerMetaManager.setDcMetaCache(dcMetaCache);
		Mockito.when(dcMetaCache.getCurrentDc()).thenReturn(getDc());
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
		currentMetaServerMetaManager.addMetaServerStateChangeHandler(handler);
		currentMetaServerMetaManager.routeChanges();

		verify(handler, times(times)).keeperMasterChanged(eq(clusterId), anyString(), any());
	}


	@Test
	public void testSetPeerMaster() {
		currentMetaServerMetaManager.addMetaServerStateChangeHandler(handler);
		currentMetaServerMetaManager.setCurrentMeta(currentMeta);

		doAnswer(invocation -> {
			String paramClusterId = invocation.getArgumentAt(0, String.class);
			String paramShardId = invocation.getArgumentAt(1, String.class);
			RedisMeta paramRedis = invocation.getArgumentAt(2, RedisMeta.class);

			Assert.assertEquals(getClusterId(), paramClusterId);
			Assert.assertEquals(getShardId(), paramShardId);
			Assert.assertEquals(new RedisMeta().setGid(1L).setIp("127.0.0.1").setPort(6379), paramRedis);

			return null;
		}).when(currentMeta).setCurrentCRDTMaster(anyString(), anyString(), any());

		doAnswer(invocation -> {
			String paramDcId = invocation.getArgumentAt(0, String.class);
			String paramClusterId = invocation.getArgumentAt(1, String.class);
			String paramShardId = invocation.getArgumentAt(2, String.class);
			RedisMeta paramRedis = invocation.getArgumentAt(3, RedisMeta.class);

			Assert.assertEquals(upstreamDc, paramDcId);
			Assert.assertEquals(getClusterId(), paramClusterId);
			Assert.assertEquals(getShardId(), paramShardId);
			Assert.assertEquals(new RedisMeta().setGid(2L).setIp("127.0.0.2").setPort(6379), paramRedis);

			return null;
		}).when(currentMeta).setPeerMaster(anyString(), anyString(), anyString(), any());

		currentMetaServerMetaManager.setCurrentCRDTMaster(getClusterId(), getShardId(), 1, "127.0.0.1", 6379);
		verify(currentMeta, times(1)).setCurrentCRDTMaster(anyString(), anyString(), any());
		verify(handler, times(1)).currentMasterChanged(getClusterId(), getShardId());

		currentMetaServerMetaManager.setPeerMaster(upstreamDc, getClusterId(), getShardId(), 2, "127.0.0.2", 6379);
		verify(currentMeta, times(1)).setPeerMaster(anyString(), anyString(), anyString(), any());
		verify(handler, times(1)).peerMasterChanged(upstreamDc, getClusterId(), getShardId());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetCurrentDcPeerMaster() {
		currentMetaServerMetaManager.setPeerMaster(getDc(), getClusterId(), getShardId(), 1, "127.0.0.1", 6379);
	}
	
	@Test
	public void testUpdateOneWayClusterMeta() {
		currentMetaServerMetaManager = spy(new DefaultCurrentMetaManager());
		currentMetaServerMetaManager.setSlotManager(slotManager);
		currentMetaServerMetaManager.setDcMetaCache(dcMetaCache);
		currentMetaServerMetaManager.setCurrentMeta(currentMeta);
		currentMetaServerMetaManager.setCurrentClusterServer(currentClusterServer);
		currentMetaServerMetaManager.addObserver(observer);
		String clusterName = "cluster1";
		Mockito.when(currentClusterServer.hasKey(clusterName)).thenReturn(true);
		
		DcMeta currentDcMeta = new DcMeta().setId("jq");
		ClusterMeta currentClusterMeta = new ClusterMeta().setType(ClusterType.ONE_WAY.name()).setId(clusterName).setActiveDc("oy");
		ShardMeta currentShardMeta = new ShardMeta().setId("cluster1_1");
		RedisMeta currentMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379);
		RedisMeta currentSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setMaster("127.0.0.1:6379");
		KeeperMeta currentActive = new KeeperMeta().setIp("127.0.0.1").setPort(16379).setActive(true);
		KeeperMeta currentKeeper= new KeeperMeta().setIp("127.0.0.1").setPort(16380);
		currentShardMeta.addRedis(currentMaster).addRedis(currentSlave).addKeeper(currentActive).addKeeper(currentKeeper);
		currentClusterMeta.addShard(currentShardMeta);
		currentDcMeta.addCluster(currentClusterMeta);


		Mockito.when(dcMetaCache.getClusterMeta(clusterName)).thenReturn(currentClusterMeta);
		
		//init
		currentMetaServerMetaManager.update(DcMetaComparator.buildClusterChanged(null, currentClusterMeta), null);
		doAnswer(invocation -> {
			Object node = invocation.getArgumentAt(0, Object.class);
			Assert.assertTrue(node instanceof NodeAdded);
			return null;
		}).when(observer).update(any(), any());
		verify(currentMeta, times(1)).addCluster(currentClusterMeta);
		verify(observer, times(1)).update(any(), any());
		
		DcMeta futureDcMeta = new DcMeta().setId("jq");
		ClusterMeta futureClusterMeta = new ClusterMeta().setType(ClusterType.ONE_WAY.name()).setId(clusterName).setActiveDc("oy");
		ShardMeta futureShardMeta = new ShardMeta().setId("cluster1_1");
		RedisMeta futureMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379);
		RedisMeta futureSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setMaster("127.0.0.1:6379");
		KeeperMeta futureActive = new KeeperMeta().setIp("127.0.0.1").setPort(1630).setActive(true);
		KeeperMeta futureKeeper= new KeeperMeta().setIp("127.0.0.1").setPort(16379);
		futureShardMeta.addRedis(futureMaster).addRedis(futureSlave).addKeeper(futureActive).addKeeper(futureKeeper);
		currentClusterMeta.addShard(futureShardMeta);
		futureDcMeta.addCluster(futureClusterMeta);
		
		DcMetaComparator dcMetaComparator = new DcMetaComparator(currentDcMeta, futureDcMeta);
		dcMetaComparator.compare();
		Mockito.when(currentMeta.hasCluster(clusterName)).thenReturn(true);
		doAnswer(invocation -> {
			Object node = invocation.getArgumentAt(0, Object.class);
			Assert.assertTrue(node instanceof ClusterMetaComparator);
			ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) node;
			Assert.assertEquals(clusterMetaComparator.getFuture().getActiveDc(), "oy");
			return null;
		}).when(observer).update(any(),any());
		currentMetaServerMetaManager.update(dcMetaComparator, null);
	}

	@Test
	public void testUpdateBiDirectionClusterMeta() {
		currentMetaServerMetaManager = spy(new DefaultCurrentMetaManager());
		currentMetaServerMetaManager.setSlotManager(slotManager);
		currentMetaServerMetaManager.setDcMetaCache(dcMetaCache);
		currentMetaServerMetaManager.setCurrentMeta(currentMeta);
		currentMetaServerMetaManager.setCurrentClusterServer(currentClusterServer);
		currentMetaServerMetaManager.addObserver(observer);
		String clusterName = "cluster1";
		Mockito.when(currentClusterServer.hasKey(clusterName)).thenReturn(true);

		DcMeta currentDcMeta = new DcMeta().setId("jq");
		ClusterMeta currentClusterMeta = new ClusterMeta().setType(ClusterType.BI_DIRECTION.name()).setId(clusterName).setDcs("jq,oy");
		ShardMeta currentShardMeta = new ShardMeta().setId("cluster1_1");
		RedisMeta currentMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
		RedisMeta currentSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1L).setMaster("127.0.0.1:6379");
		KeeperMeta currentActive = new KeeperMeta().setIp("127.0.0.1").setPort(16379).setActive(true);
		KeeperMeta currentKeeper= new KeeperMeta().setIp("127.0.0.1").setPort(16380);
		currentShardMeta.addRedis(currentMaster).addRedis(currentSlave).addKeeper(currentActive).addKeeper(currentKeeper);
		currentClusterMeta.addShard(currentShardMeta);
		currentDcMeta.addCluster(currentClusterMeta);


		Mockito.when(dcMetaCache.getClusterMeta(clusterName)).thenReturn(currentClusterMeta);
		doAnswer(invocation -> {
			Object node = invocation.getArgumentAt(0, Object.class);
			Assert.assertTrue(node instanceof NodeAdded);
			return null;
		}).when(observer).update(any(), any());
		//init
		currentMetaServerMetaManager.update(DcMetaComparator.buildClusterChanged(null, currentClusterMeta), null);
		
		verify(currentMeta, times(1)).addCluster(currentClusterMeta);
		verify(observer, times(1)).update(any(), any());
		verify(currentMeta, times(1)).updateClusterRoutes(any(), any());
		
		DcMeta futureDcMeta = new DcMeta().setId("jq").addRoute(new RouteMeta().setId(1));
		ClusterMeta futureClusterMeta = new ClusterMeta().setType(ClusterType.BI_DIRECTION.name()).setId(clusterName).setDcs("jq,oy,fq");
		ShardMeta futureShardMeta = new ShardMeta().setId("cluster1_1");
		RedisMeta futureMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
		RedisMeta futureSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1L).setMaster("127.0.0.1:6379");
		futureShardMeta.addRedis(futureMaster).addRedis(futureSlave);
		currentClusterMeta.addShard(futureShardMeta);
		futureDcMeta.addCluster(futureClusterMeta);

		DcMetaComparator dcMetaComparator = new DcMetaComparator(currentDcMeta, futureDcMeta);
		dcMetaComparator.compare();
		Mockito.when(currentMeta.hasCluster(clusterName)).thenReturn(true);
		doAnswer(invocation -> {
			Object node = invocation.getArgumentAt(0, Object.class);
			Assert.assertTrue(node instanceof ClusterMetaComparator);
			ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) node;
			Assert.assertEquals(clusterMetaComparator.getFuture().getDcs(), "jq,oy,fq");
			return null;
		}).when(observer).update(any(),any());
		currentMetaServerMetaManager.update(dcMetaComparator, null);
		//no exec addCluster
		verify(currentMeta, times(1)).addCluster(any());
		verify(currentMeta, times(2)).updateClusterRoutes(any(), any());
		
		DcRouteMetaComparator dcRouteMetaComparator = new DcRouteMetaComparator(currentDcMeta, futureDcMeta);
		dcRouteMetaComparator.compare();
		Set<String> allClusters = new HashSet<>();
		allClusters.add(clusterName);
		Mockito.when(currentMeta.allClusters()).thenReturn(allClusters);
		currentMetaServerMetaManager.update(dcRouteMetaComparator, null);
		//no exec addCluster
		verify(currentMeta, times(1)).addCluster(any());
		verify(currentMeta, times(3)).updateClusterRoutes(any(), any());
	}

	@Test
	public void testChangeClusterType() {
		currentMetaServerMetaManager = spy(new DefaultCurrentMetaManager());
		currentMetaServerMetaManager.setSlotManager(slotManager);
		currentMetaServerMetaManager.setDcMetaCache(dcMetaCache);
		currentMetaServerMetaManager.setCurrentMeta(currentMeta);
		currentMetaServerMetaManager.setCurrentClusterServer(currentClusterServer);
		currentMetaServerMetaManager.addObserver(observer);
		String clusterName = "cluster1";
		Mockito.when(currentClusterServer.hasKey(clusterName)).thenReturn(true);

		DcMeta currentDcMeta = new DcMeta().setId("jq");
		ClusterMeta currentClusterMeta = new ClusterMeta().setType(ClusterType.ONE_WAY.name()).setId(clusterName).setActiveDc("oy");
		ShardMeta currentShardMeta = new ShardMeta().setId("cluster1_1");
		RedisMeta currentMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379);
		RedisMeta currentSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setMaster("127.0.0.1:6379");
		KeeperMeta currentActive = new KeeperMeta().setIp("127.0.0.1").setPort(16379).setActive(true);
		KeeperMeta currentKeeper= new KeeperMeta().setIp("127.0.0.1").setPort(16380);
		currentShardMeta.addRedis(currentMaster).addRedis(currentSlave).addKeeper(currentActive).addKeeper(currentKeeper);
		currentClusterMeta.addShard(currentShardMeta);
		currentDcMeta.addCluster(currentClusterMeta);


		Mockito.when(dcMetaCache.getClusterMeta(clusterName)).thenReturn(currentClusterMeta);

		//init
		currentMetaServerMetaManager.update(DcMetaComparator.buildClusterChanged(null, currentClusterMeta), null);
		doAnswer(invocation -> {
			Object node = invocation.getArgumentAt(0, Object.class);
			Assert.assertTrue(node instanceof NodeAdded);
			return null;
		}).when(observer).update(any(), any());
		verify(currentMeta, times(1)).addCluster(currentClusterMeta);
		verify(observer, times(1)).update(any(), any());

		DcMeta futureDcMeta = new DcMeta().setId("jq").addRoute(new RouteMeta().setId(1));
		ClusterMeta futureClusterMeta = new ClusterMeta().setType(ClusterType.BI_DIRECTION.name()).setId(clusterName).setDcs("jq,oy,fq");
		ShardMeta futureShardMeta = new ShardMeta().setId("cluster1_1");
		RedisMeta futureMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
		RedisMeta futureSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1L).setMaster("127.0.0.1:6379");
		futureShardMeta.addRedis(futureMaster).addRedis(futureSlave);
		currentClusterMeta.addShard(futureShardMeta);
		futureDcMeta.addCluster(futureClusterMeta);

		DcMetaComparator dcMetaComparator = new DcMetaComparator(currentDcMeta, futureDcMeta);
		dcMetaComparator.compare();
		Mockito.when(currentMeta.hasCluster(clusterName)).thenReturn(true);
		currentMetaServerMetaManager.update(dcMetaComparator, null);
		//remove
		verify(currentMeta, times(1)).removeCluster(clusterName);
		//add
		verify(currentMeta, times(2)).addCluster(currentClusterMeta);
	}
}
