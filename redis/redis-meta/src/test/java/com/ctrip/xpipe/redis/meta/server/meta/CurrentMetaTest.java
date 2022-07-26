package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author wenchao.meng
 *
 * Sep 6, 2016
 */
public class CurrentMetaTest extends AbstractMetaServerTest{
	
	private ClusterMeta clusterMeta;
	private ClusterMeta biClusterMeta;
	private CurrentMeta currentMeta;
	
	private String clusterId, shardId;
	private Long clusterDbId , shardDbId;
	private String biClusterId, biShardId;
	private Long biClusterDbId, bishardDbId;
	private AtomicInteger releaseCount = new AtomicInteger();
	
	@Before
	public void beforeCurrentMetaTest(){
		clusterMeta = (ClusterMeta) getDcMeta(getDc()).getClusters().values().toArray()[0];
		biClusterMeta = (ClusterMeta) getDcMeta(getDc()).getClusters().values().toArray()[1];
		currentMeta = new CurrentMeta();
		currentMeta.addCluster(clusterMeta);
		currentMeta.addCluster(biClusterMeta);
		List<RouteMeta> routes = getDcMeta(getDc()).getRoutes(); 
//		currentMeta.updateClusterRoutes(biClusterMeta, routes);

		clusterId = clusterMeta.getId();
		shardId = clusterMeta.getShards().keySet().iterator().next();
		clusterDbId = clusterMeta.getDbId();
		shardDbId = clusterMeta.getShards().get(shardId).getDbId();

		biClusterId = biClusterMeta.getId();
		biShardId = biClusterMeta.getShards().keySet().iterator().next();
		biClusterDbId = biClusterMeta.getDbId();
		bishardDbId = biClusterMeta.getShards().get(biShardId).getDbId();
	}

	@Test
	public void testAddResourceConcurrently() throws Exception {
		int concurrentSize = 1000;
		AtomicInteger releaseCount = new AtomicInteger(0);
		CountDownLatch latch = new CountDownLatch(concurrentSize);
		CyclicBarrier barrier = new CyclicBarrier(concurrentSize);
		IntStream.range(0, concurrentSize).forEach(i -> {
			new Thread(() -> {
				try {
					barrier.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (BrokenBarrierException e) {
					e.printStackTrace();
				}

				currentMeta.addResource(clusterDbId, shardDbId, new Releasable() {
					@Override
					public void release() throws Exception {
						releaseCount.incrementAndGet();
					}
				});
				latch.countDown();
			}).start();
		});

		latch.await(3, TimeUnit.SECONDS);
		currentMeta.release();

		Assert.assertEquals(concurrentSize, releaseCount.get());
	}
	
	@Test
	public void testGetKeeperMaster(){
		
		Pair<String, Integer> keeperMaster = new Pair<>("localhost", 6379);
		currentMeta.setKeeperMaster(clusterDbId, shardDbId, keeperMaster);
		
		Pair<String, Integer> gotMaster = currentMeta.getKeeperMaster(clusterDbId, shardDbId);
		Assert.assertEquals(keeperMaster, gotMaster);
		Assert.assertTrue(keeperMaster != gotMaster);
		
		
		keeperMaster.setKey("127.0.0.2");
		gotMaster = currentMeta.getKeeperMaster(clusterDbId, shardDbId);
		Assert.assertNotEquals(keeperMaster, gotMaster);
		Assert.assertTrue(keeperMaster != gotMaster);
	}
	
	@Test
	public void testDefaultMaster(){
		String clusterId = getClusterId(), shardId = getShardId();
		String activeDc = getDcMeta(getDc()).getClusters().get(clusterId).getActiveDc();
				
		for(String dc : getDcs()){
			CurrentMeta currentMeta = new CurrentMeta();
			ClusterMeta clusterMeta = getDcMeta(dc).getClusters().get(clusterId); 
			currentMeta.addCluster(clusterMeta);
			Pair<String, Integer> keeperMaster = currentMeta.getKeeperMaster(clusterDbId, shardDbId);
			
			logger.info("[testDefaultMaster]{},{},{}-{}", dc, clusterDbId, shardDbId, keeperMaster);
			if(dc.equals(activeDc)){
				Assert.assertEquals(new Pair<String, Integer>("127.0.0.1", 6379), keeperMaster);
			}else{
				Assert.assertEquals(null, keeperMaster);
			}
		}
		
	}
	
	@Test
	public void testRelease(){
		
		currentMeta.addResource(clusterDbId, shardDbId, new Releasable() {
			
			@Override
			public void release() throws Exception {
				releaseCount.incrementAndGet();
			}
		});
		
		currentMeta.removeCluster(clusterDbId);
		Assert.assertEquals(1, releaseCount.get());
		
	}
	
	
	@Test
	public void testToString(){
		
		List<KeeperMeta> allKeepers = getDcKeepers(getDc(), clusterId, shardId);
		
		currentMeta.setSurviveKeepers(clusterDbId, shardDbId, allKeepers, allKeepers.get(0));
		currentMeta.addResource(clusterDbId, shardDbId, new Releasable() {
			
			@Override
			public void release() throws Exception {
				
			}
		});
		
		String json = currentMeta.toString();
		CurrentMeta de = CurrentMeta.fromJson(json);
		Assert.assertEquals(json, de.toString());
		
		Assert.assertTrue(currentMeta.hasCluster(clusterDbId));
		for(ShardMeta shardMeta : clusterMeta.getShards().values()){
			Assert.assertTrue(currentMeta.hasShard(clusterDbId, shardMeta.getDbId()));
		}
	}

	@Test
	public void testSetInfo(){

		//set survice keepers
		Assert.assertEquals(0, currentMeta.getSurviveKeepers(clusterDbId, shardDbId).size());
		Assert.assertEquals(null, currentMeta.getKeeperActive(clusterDbId, shardDbId));
		
		List<KeeperMeta> allKeepers = getDcKeepers(getDc(), clusterId, shardId);
		KeeperMeta active = allKeepers.get(0);
		currentMeta.setSurviveKeepers(clusterDbId, shardDbId, allKeepers, active);
		Assert.assertEquals(allKeepers.size(), currentMeta.getSurviveKeepers(clusterDbId, shardDbId).size());
		active.setActive(true);
		Assert.assertEquals(active, currentMeta.getKeeperActive(clusterDbId, shardDbId));
		
		//set keeper active
		
		KeeperMeta keeperMeta =  getDcKeepers(getDc(), clusterId, shardId).get(1);
		boolean result = currentMeta.setKeeperActive(clusterDbId, shardDbId, keeperMeta);
		Assert.assertTrue(result);
		keeperMeta.setActive(true);
		Assert.assertEquals(keeperMeta, currentMeta.getKeeperActive(clusterDbId, shardDbId));
		Assert.assertFalse(currentMeta.setKeeperActive(clusterDbId, shardDbId, keeperMeta));
		
		//set keeper active not exist
		keeperMeta.setIp(randomString(10));
		try{
			currentMeta.setKeeperActive(clusterDbId, shardDbId, keeperMeta);
			Assert.fail();
		}catch(Exception e){
			
		}
		

		Assert.assertEquals(new Pair<String, Integer>("127.0.0.1", 6379), currentMeta.getKeeperMaster(clusterDbId, shardDbId));
		Pair<String, Integer> keeperMaster = new Pair<String, Integer>("localhost", randomPort());
		currentMeta.setKeeperMaster(clusterDbId, shardDbId, keeperMaster);
		Assert.assertEquals(keeperMaster, currentMeta.getKeeperMaster(clusterDbId, shardDbId));

		
		
	}
	
	@Test
	public void testChange(){
		
		ClusterMeta future = MetaClone.clone(clusterMeta);
		String newShardId = randomString(100);
		Long newShardDbId = Math.abs(randomLong());
		ShardMeta shardMeta = future.getShards().remove(shardId);
		shardMeta.setId(newShardId).setDbId(newShardDbId);
		future.addShard(shardMeta);
		ClusterMetaComparator comparator = new ClusterMetaComparator(clusterMeta, future);
		comparator.compare();
		
		currentMeta.changeCluster(comparator);
		Assert.assertFalse(currentMeta.hasShard(clusterDbId, shardDbId));
		Assert.assertTrue(currentMeta.hasShard(clusterDbId, newShardDbId));
	}

	@Test
	public void testSetInfoForCRDTCluster() {

		Assert.assertEquals(0, currentMeta.getUpstreamPeerDcs(biClusterDbId, bishardDbId).size());
		Assert.assertEquals(0, currentMeta.getAllPeerMasters(biClusterDbId, bishardDbId).size());
		Assert.assertNull(currentMeta.getCurrentCRDTMaster(biClusterDbId, bishardDbId));

		// set PeerMaster
		RedisMeta redisMeta = new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L);
		currentMeta.setCurrentCRDTMaster(biClusterDbId, bishardDbId, redisMeta);
		redisMeta.setIp("10.0.0.2");
		currentMeta.setPeerMaster("remote-dc", biClusterDbId, bishardDbId, redisMeta);
		Assert.assertEquals(1, currentMeta.getUpstreamPeerDcs(biClusterDbId, bishardDbId).size());
		Assert.assertEquals(1, currentMeta.getAllPeerMasters(biClusterDbId, bishardDbId).size());
		Assert.assertEquals(Sets.newHashSet("remote-dc"), currentMeta.getUpstreamPeerDcs(biClusterDbId, bishardDbId));
		Assert.assertEquals(new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L), currentMeta.getCurrentCRDTMaster(biClusterDbId, bishardDbId));
		Assert.assertEquals(new RedisMeta().setIp("10.0.0.2").setPort(6379).setGid(1L), currentMeta.getPeerMaster("remote-dc", biClusterDbId, bishardDbId));

		// remove PeerMaster
		currentMeta.removePeerMaster("remote-dc", biClusterDbId, bishardDbId);
		Assert.assertEquals(0, currentMeta.getUpstreamPeerDcs(biClusterDbId, bishardDbId).size());
		Assert.assertEquals(0, currentMeta.getAllPeerMasters(biClusterDbId, bishardDbId).size());
		Assert.assertEquals(new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L), currentMeta.getCurrentCRDTMaster(biClusterDbId, bishardDbId));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetPeerMasterWithErrorType() {
		currentMeta.setPeerMaster(getDc(), clusterDbId, shardDbId, new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetKeeperMasterWithErrorType() {
		currentMeta.setKeeperMaster(biClusterDbId, bishardDbId, Pair.of("127.0.0.1", 6379));
	}

	@Test
	public void testGetCurrentMaster() {
		Assert.assertEquals(new RedisMeta().setIp("127.0.0.1").setPort(6379), currentMeta.getCurrentMaster(clusterDbId, shardDbId));
		Assert.assertNull(currentMeta.getCurrentMaster(biClusterDbId, bishardDbId));

		currentMeta.setCurrentCRDTMaster(biClusterDbId, bishardDbId, new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L));
		Assert.assertEquals(new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L), currentMeta.getCurrentMaster(biClusterDbId, bishardDbId));
	}

}
	
		
