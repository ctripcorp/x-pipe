package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultDcMetaManager;
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
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 *         Aug 31, 2016
 */
@RunWith(MockitoJUnitRunner.Silent.class)
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
		currentMetaServerMetaManager.setCurrentClusterServer(currentClusterServer);
		currentMetaServerMetaManager.addObserver(observer);
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
		Long clusterDbId = getClusterDbId();

		when(currentMetaServerMetaManager.allClusters()).thenReturn(Sets.newHashSet(clusterDbId));
		Assert.assertFalse(currentMetaServerMetaManager.allClusters().isEmpty());

		doReturn(Pair.from("127.0.0.1", randomPort())).when(currentMetaServerMetaManager).getKeeperMaster(anyLong(), anyLong());

		when(dcMetaCache.randomRoute(clusterDbId)).thenReturn(new RouteMeta(1000).setTag(Route.TAG_META));
		when(dcMetaCache.getClusterMeta(clusterDbId)).thenReturn(getCluster(getDcs()[0], clusterId));

		int times = getCluster(getDcs()[0], clusterId).getShards().size();
		currentMetaServerMetaManager.addMetaServerStateChangeHandler(handler);
		currentMetaServerMetaManager.routeChanges();

		verify(handler, times(times)).keeperMasterChanged(eq(clusterDbId), anyLong(), any());
	}


	@Test
	public void testSetPeerMaster() {
		currentMetaServerMetaManager.addMetaServerStateChangeHandler(handler);
		currentMetaServerMetaManager.setCurrentMeta(currentMeta);

		doAnswer(invocation -> {
			Long paramClusterId = invocation.getArgument(0, Long.class);
			Long paramShardId = invocation.getArgument(1, Long.class);
			RedisMeta paramRedis = invocation.getArgument(2, RedisMeta.class);

			Assert.assertEquals(getClusterDbId(), paramClusterId);
			Assert.assertEquals(getShardDbId(), paramShardId);
			Assert.assertEquals(new RedisMeta().setGid(1L).setIp("127.0.0.1").setPort(6379), paramRedis);

			return null;
		}).when(currentMeta).setCurrentCRDTMaster(anyLong(), anyLong(), any());

		doAnswer(invocation -> {
			String paramDcId = invocation.getArgument(0, String.class);
			Long paramClusterId = invocation.getArgument(1, Long.class);
			Long paramShardId = invocation.getArgument(2, Long.class);
			RedisMeta paramRedis = invocation.getArgument(3, RedisMeta.class);

			Assert.assertEquals(upstreamDc, paramDcId);
			Assert.assertEquals(getClusterDbId(), paramClusterId);
			Assert.assertEquals(getShardDbId(), paramShardId);
			Assert.assertEquals(new RedisMeta().setGid(2L).setIp("127.0.0.2").setPort(6379), paramRedis);

			return null;
		}).when(currentMeta).setPeerMaster(anyString(), anyLong(), anyLong(), any());

		currentMetaServerMetaManager.setCurrentCRDTMaster(getClusterDbId(), getShardDbId(), 1, "127.0.0.1", 6379);
		verify(currentMeta, times(1)).setCurrentCRDTMaster(anyLong(), anyLong(), any());
		verify(handler, times(1)).currentMasterChanged(getClusterDbId(), getShardDbId());

		currentMetaServerMetaManager.setPeerMaster(upstreamDc, getClusterDbId(), getShardDbId(), 2, "127.0.0.2", 6379);
		verify(currentMeta, times(1)).setPeerMaster(anyString(), anyLong(), anyLong(), any());
		verify(handler, times(1)).peerMasterChanged(upstreamDc, getClusterDbId(), getShardDbId());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetCurrentDcPeerMaster() {
		currentMetaServerMetaManager.setPeerMaster(getDc(), getClusterDbId(), getShardDbId(), 1, "127.0.0.1", 6379);
	}

	private void prepareData() {
		DcMeta current = getDcMeta(getDc());
		for (ClusterMeta clusterMeta: current.getClusters().values()) {
			when(dcMetaCache.getClusterMeta(clusterMeta.getDbId())).thenReturn(clusterMeta);
			when(dcMetaCache.getClusterType(clusterMeta.getDbId())).thenReturn(ClusterType.lookup(clusterMeta.getType()));
		}

		DcMetaComparator comparator = new DcMetaComparator(new DcMeta(getDc()), getDcMeta(getDc()));
		comparator.compare();
		currentMetaServerMetaManager.update(comparator, null);
	}

	@Test
	public void testClusterNameChangeAndBecomeInterested_addCluster() {
		DcMeta future = MetaClone.clone(getDcMeta(getDc()));
		Set<Long> allClusters = future.getClusters().values().stream().map(ClusterMeta::getDbId).collect(Collectors.toSet());

		ClusterMeta clusterMeta = future.getClusters().values().iterator().next();
		Long clusterDbId = clusterMeta.getDbId();
		String clusterName1 = clusterMeta.getId(), clusterName2 = clusterMeta.getId() + "1";

		when(currentClusterServer.hasKey(anyString())).thenReturn(true);
		when(currentClusterServer.hasKey(clusterName1)).thenReturn(false);
		when(currentClusterServer.hasKey(clusterName2)).thenReturn(true);
		prepareData();
		allClusters.remove(clusterDbId);
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());

		clusterMeta.setId(clusterName2);
		DcMetaComparator comparator = new DcMetaComparator(getDcMeta(getDc()), future);
		comparator.compare();
		currentMetaServerMetaManager.update(comparator, null);
		allClusters.add(clusterDbId);
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());
	}

	@Test
	public void testClusterNameChangeAndLossInterested_removeCluster() {
		DcMeta future = MetaClone.clone(getDcMeta(getDc()));
		Set<Long> allClusters = future.getClusters().values().stream().map(ClusterMeta::getDbId).collect(Collectors.toSet());

		ClusterMeta clusterMeta = future.getClusters().values().iterator().next();
		Long clusterDbId = clusterMeta.getDbId();
		String clusterName1 = clusterMeta.getId(), clusterName2 = clusterMeta.getId() + "1";

		when(currentClusterServer.hasKey(anyString())).thenReturn(true);
		when(currentClusterServer.hasKey(clusterName2)).thenReturn(false);
		prepareData();
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());

		clusterMeta.setId(clusterName2);
		DcMetaComparator comparator = new DcMetaComparator(getDcMeta(getDc()), future);
		comparator.compare();
		currentMetaServerMetaManager.update(comparator, null);
		allClusters.remove(clusterDbId);
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());
	}

	@Test
	public void testClusterNameChangeAndAllInterested_handleClusterChange() {
		doAnswer(inv -> {
			logger.info("[notifyObserver] {}", inv.getArguments());
			return null;
		}).when(observer).update(any(), any());
		DcMeta future = MetaClone.clone(getDcMeta(getDc()));
		Set<Long> allClusters = future.getClusters().values().stream().map(ClusterMeta::getDbId).collect(Collectors.toSet());

		ClusterMeta clusterMeta = future.getClusters().values().iterator().next();
		Long clusterDbId = clusterMeta.getDbId();
		String clusterName1 = clusterMeta.getId(), clusterName2 = clusterMeta.getId() + "1";

		when(currentClusterServer.hasKey(anyString())).thenReturn(true);
		prepareData();
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());

		clusterMeta.setId(clusterName2);
		DcMetaComparator comparator = new DcMetaComparator(getDcMeta(getDc()), future);
		comparator.compare();
		currentMetaServerMetaManager.update(comparator, null);
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());
		verify(observer).update(Matchers.isA(ClusterMetaComparator.class), Matchers.eq(currentMetaServerMetaManager));
	}

	@Test
	public void testClusterNameChangeAndAllNotInterested_doNothing() {
		DcMeta future = MetaClone.clone(getDcMeta(getDc()));
		Set<Long> allClusters = future.getClusters().values().stream().map(ClusterMeta::getDbId).collect(Collectors.toSet());

		ClusterMeta clusterMeta = future.getClusters().values().iterator().next();
		Long clusterDbId = clusterMeta.getDbId();
		String clusterName1 = clusterMeta.getId(), clusterName2 = clusterMeta.getId() + "1";

		when(currentClusterServer.hasKey(anyString())).thenReturn(true);
		when(currentClusterServer.hasKey(clusterName1)).thenReturn(false);
		when(currentClusterServer.hasKey(clusterName2)).thenReturn(false);
		prepareData();
		allClusters.remove(clusterDbId);
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());

		clusterMeta.setId(clusterName2);
		DcMetaComparator comparator = new DcMetaComparator(getDcMeta(getDc()), future);
		comparator.compare();
		currentMetaServerMetaManager.update(comparator, null);
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());
		verify(observer, never()).update(Matchers.isA(ClusterMetaComparator.class), Matchers.eq(currentMetaServerMetaManager));
	}

	@Test
	public void testCurrentMetaMiss_addCluster() {
		when(currentClusterServer.hasKey(anyString())).thenReturn(true);
		Set<Long> allClusters = getDcMeta(getDc()).getClusters().values().stream().map(ClusterMeta::getDbId).collect(Collectors.toSet());
		prepareData();
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());

		Set<ClusterMeta> clusters = new HashSet<>(getDcMeta(getDc()).getClusters().values());
		ClusterMeta newCluster = new ClusterMeta(randomString()).setDbId(Math.abs(randomLong()));
		clusters.add(newCluster);
		when(dcMetaCache.getClusters()).thenReturn(clusters);
		when(dcMetaCache.getClusterMeta(newCluster.getDbId())).thenReturn(newCluster);

		currentMetaServerMetaManager.checkCurrentMetaMissOrRedundant();
		allClusters.add(newCluster.getDbId());
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());
	}

	@Test
	public void testCurrentMetaRedundant_removeCluster() {
		when(currentClusterServer.hasKey(anyString())).thenReturn(true);
		Set<Long> allClusters = getDcMeta(getDc()).getClusters().values().stream().map(ClusterMeta::getDbId).collect(Collectors.toSet());
		prepareData();
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());

		Set<ClusterMeta> clusters = new HashSet<>(getDcMeta(getDc()).getClusters().values());
		ClusterMeta removedCluster = clusters.iterator().next();
		when(dcMetaCache.getClusters()).thenReturn(clusters);
		when(currentClusterServer.hasKey(removedCluster.getId())).thenReturn(false);

		currentMetaServerMetaManager.checkCurrentMetaMissOrRedundant();
		allClusters.remove(removedCluster.getDbId());
		Assert.assertEquals(allClusters, currentMetaServerMetaManager.allClusters());
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
		Long clusterDbId = 1L;
		Long shardDbId = 1L;
		Mockito.when(currentClusterServer.hasKey(clusterName)).thenReturn(true);
		
		DcMeta currentDcMeta = new DcMeta().setId("jq");
		ClusterMeta currentClusterMeta = new ClusterMeta().setType(ClusterType.ONE_WAY.name()).setId(clusterName).setActiveDc("oy").setDbId(clusterDbId);
		ShardMeta currentShardMeta = new ShardMeta().setId("cluster1_1").setDbId(shardDbId);
		RedisMeta currentMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379);
		RedisMeta currentSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setMaster("127.0.0.1:6379");
		KeeperMeta currentActive = new KeeperMeta().setIp("127.0.0.1").setPort(16379).setActive(true);
		KeeperMeta currentKeeper= new KeeperMeta().setIp("127.0.0.1").setPort(16380);
		currentShardMeta.addRedis(currentMaster).addRedis(currentSlave).addKeeper(currentActive).addKeeper(currentKeeper);
		currentClusterMeta.addShard(currentShardMeta);
		currentDcMeta.addCluster(currentClusterMeta);


		Mockito.when(dcMetaCache.getClusterMeta(clusterDbId)).thenReturn(currentClusterMeta);
		
		//init
		currentMetaServerMetaManager.update(DcMetaComparator.buildClusterChanged(null, currentClusterMeta), null);
		verify(currentMeta, times(1)).addCluster(currentClusterMeta);
		verify(observer, times(1)).update(any(NodeAdded.class), any());
		
		DcMeta futureDcMeta = new DcMeta().setId("jq");
		ClusterMeta futureClusterMeta = new ClusterMeta().setType(ClusterType.ONE_WAY.name()).setId(clusterName).setDbId(clusterDbId).setActiveDc("oy");
		ShardMeta futureShardMeta = new ShardMeta().setId("cluster1_1").setDbId(shardDbId);
		RedisMeta futureMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379);
		RedisMeta futureSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setMaster("127.0.0.1:6379");
		KeeperMeta futureActive = new KeeperMeta().setIp("127.0.0.1").setPort(1630).setActive(true);
		KeeperMeta futureKeeper= new KeeperMeta().setIp("127.0.0.1").setPort(16379);
		futureShardMeta.addRedis(futureMaster).addRedis(futureSlave).addKeeper(futureActive).addKeeper(futureKeeper);
		currentClusterMeta.addShard(futureShardMeta);
		futureDcMeta.addCluster(futureClusterMeta);
		
		DcMetaComparator dcMetaComparator = new DcMetaComparator(currentDcMeta, futureDcMeta);
		dcMetaComparator.compare();
		Mockito.when(currentMeta.hasCluster(clusterDbId)).thenReturn(true);
		doAnswer(invocation -> {
			Object node = invocation.getArgument(0, Object.class);
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
		Long clusterDbId = 2L;
		Long shardDbId = 2L;
		Mockito.when(currentClusterServer.hasKey(clusterName)).thenReturn(true);

		DcMeta currentDcMeta = new DcMeta().setId("jq");
		ClusterMeta currentClusterMeta = new ClusterMeta().setType(ClusterType.BI_DIRECTION.name()).setId(clusterName).setDcs("jq,oy").setDbId(clusterDbId);
		ShardMeta currentShardMeta = new ShardMeta().setId("cluster1_1").setDbId(shardDbId);
		RedisMeta currentMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
		RedisMeta currentSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1L).setMaster("127.0.0.1:6379");
		KeeperMeta currentActive = new KeeperMeta().setIp("127.0.0.1").setPort(16379).setActive(true);
		KeeperMeta currentKeeper= new KeeperMeta().setIp("127.0.0.1").setPort(16380);
		currentShardMeta.addRedis(currentMaster).addRedis(currentSlave).addKeeper(currentActive).addKeeper(currentKeeper);
		currentClusterMeta.addShard(currentShardMeta);
		currentDcMeta.addCluster(currentClusterMeta);


		Mockito.when(dcMetaCache.getClusterMeta(clusterDbId)).thenReturn(currentClusterMeta);
		doAnswer(invocation -> {
			Object node = invocation.getArgument(0, Object.class);
			Assert.assertTrue(node instanceof NodeAdded);
			return null;
		}).when(observer).update(any(), any());
		//init
		currentMetaServerMetaManager.update(DcMetaComparator.buildClusterChanged(null, currentClusterMeta), null);
		
		verify(currentMeta, times(1)).addCluster(currentClusterMeta);
		verify(observer, times(1)).update(any(), any());
		verify(currentMeta, times(1)).updateClusterRoutes(any(), any());
		
		DcMeta futureDcMeta = new DcMeta().setId("jq").addRoute(new RouteMeta().setId(1));
		ClusterMeta futureClusterMeta = new ClusterMeta().setType(ClusterType.BI_DIRECTION.name()).setId(clusterName).setDbId(clusterDbId).setDcs("jq,oy,fq");
		ShardMeta futureShardMeta = new ShardMeta().setId("cluster1_1").setDbId(shardDbId);
		RedisMeta futureMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
		RedisMeta futureSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1L).setMaster("127.0.0.1:6379");
		futureShardMeta.addRedis(futureMaster).addRedis(futureSlave);
		currentClusterMeta.addShard(futureShardMeta);
		futureDcMeta.addCluster(futureClusterMeta);

		DcMetaComparator dcMetaComparator = new DcMetaComparator(currentDcMeta, futureDcMeta);
		dcMetaComparator.compare();
		Mockito.when(currentMeta.hasCluster(clusterDbId)).thenReturn(true);
		doAnswer(invocation -> {
			Object node = invocation.getArgument(0, Object.class);
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
		Set<Long> allClusters = new HashSet<>();
		allClusters.add(clusterDbId);
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
		Long clusterDbId = 1L;
		Long shardDbId = 1L;
		Mockito.when(currentClusterServer.hasKey(clusterName)).thenReturn(true);

		DcMeta currentDcMeta = new DcMeta().setId("jq");
		ClusterMeta currentClusterMeta = new ClusterMeta().setType(ClusterType.ONE_WAY.name()).setId(clusterName).setActiveDc("oy").setDbId(clusterDbId);
		ShardMeta currentShardMeta = new ShardMeta().setId("cluster1_1").setDbId(shardDbId);
		RedisMeta currentMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379);
		RedisMeta currentSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setMaster("127.0.0.1:6379");
		KeeperMeta currentActive = new KeeperMeta().setIp("127.0.0.1").setPort(16379).setActive(true);
		KeeperMeta currentKeeper= new KeeperMeta().setIp("127.0.0.1").setPort(16380);
		currentShardMeta.addRedis(currentMaster).addRedis(currentSlave).addKeeper(currentActive).addKeeper(currentKeeper);
		currentClusterMeta.addShard(currentShardMeta);
		currentDcMeta.addCluster(currentClusterMeta);


		Mockito.when(dcMetaCache.getClusterMeta(clusterDbId)).thenReturn(currentClusterMeta);

		//init
		currentMetaServerMetaManager.update(DcMetaComparator.buildClusterChanged(null, currentClusterMeta), null);
		doAnswer(invocation -> {
			Object node = invocation.getArgument(0, Object.class);
			Assert.assertTrue(node instanceof NodeAdded);
			return null;
		}).when(observer).update(any(), any());
		verify(currentMeta, times(1)).addCluster(currentClusterMeta);
		verify(observer, times(1)).update(any(), any());

		DcMeta futureDcMeta = new DcMeta().setId("jq").addRoute(new RouteMeta().setId(1));
		ClusterMeta futureClusterMeta = new ClusterMeta().setType(ClusterType.BI_DIRECTION.name()).setId(clusterName).setDbId(clusterDbId).setDcs("jq,oy,fq");
		ShardMeta futureShardMeta = new ShardMeta().setId("cluster1_1").setDbId(shardDbId);
		RedisMeta futureMaster = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
		RedisMeta futureSlave = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1L).setMaster("127.0.0.1:6379");
		futureShardMeta.addRedis(futureMaster).addRedis(futureSlave);
		currentClusterMeta.addShard(futureShardMeta);
		futureDcMeta.addCluster(futureClusterMeta);

		DcMetaComparator dcMetaComparator = new DcMetaComparator(currentDcMeta, futureDcMeta);
		dcMetaComparator.compare();
		AtomicBoolean exist = new AtomicBoolean(true);
		doAnswer(inv -> {
			exist.set(false);
			return null;
		}).when(currentMeta).removeCluster(clusterDbId);
		doAnswer(inv -> exist.get()).when(currentMeta).hasCluster(clusterDbId);
		doAnswer(inv -> {
			exist.set(true);
			return null;
		}).when(currentMeta).addCluster(any());

		currentMetaServerMetaManager.update(dcMetaComparator, null);
		//remove
		verify(currentMeta, times(1)).removeCluster(clusterDbId);
		//add
		verify(currentMeta, times(2)).addCluster(currentClusterMeta);
	}
}
