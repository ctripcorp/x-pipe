package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
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
	private String biClusterId, biShardId;
	private AtomicInteger releaseCount = new AtomicInteger();
	
	@Before
	public void beforeCurrentMetaTest(){
		clusterMeta = (ClusterMeta) getDcMeta(getDc()).getClusters().values().toArray()[0];
		biClusterMeta = (ClusterMeta) getDcMeta(getDc()).getClusters().values().toArray()[1];
		currentMeta = new CurrentMeta();
		currentMeta.addCluster(clusterMeta);
		currentMeta.addCluster(biClusterMeta);
		
		clusterId = clusterMeta.getId();
		shardId = clusterMeta.getShards().keySet().iterator().next();

		biClusterId = biClusterMeta.getId();
		biShardId = biClusterMeta.getShards().keySet().iterator().next();
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

				currentMeta.addResource(clusterId, shardId, new Releasable() {
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
		currentMeta.setKeeperMaster(clusterId, shardId, keeperMaster);
		
		Pair<String, Integer> gotMaster = currentMeta.getKeeperMaster(clusterId, shardId);
		Assert.assertEquals(keeperMaster, gotMaster);
		Assert.assertTrue(keeperMaster != gotMaster);
		
		
		keeperMaster.setKey("127.0.0.2");
		gotMaster = currentMeta.getKeeperMaster(clusterId, shardId);
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
			Pair<String, Integer> keeperMaster = currentMeta.getKeeperMaster(clusterId, shardId);
			
			logger.info("[testDefaultMaster]{},{},{}-{}", dc, clusterId, shardId, keeperMaster);
			if(dc.equals(activeDc)){
				Assert.assertEquals(new Pair<String, Integer>("127.0.0.1", 6379), keeperMaster);
			}else{
				Assert.assertEquals(null, keeperMaster);
			}
		}
		
	}
	
	@Test
	public void testRelease(){
		
		currentMeta.addResource(clusterId, shardId, new Releasable() {
			
			@Override
			public void release() throws Exception {
				releaseCount.incrementAndGet();
			}
		});
		
		currentMeta.removeCluster(clusterId);
		Assert.assertEquals(1, releaseCount.get());
		
	}
	
	
	@Test
	public void testToString(){
		
		List<KeeperMeta> allKeepers = getDcKeepers(getDc(), clusterId, shardId);
		
		currentMeta.setSurviveKeepers(clusterId, shardId, allKeepers, allKeepers.get(0));
		currentMeta.addResource(clusterId, shardId, new Releasable() {
			
			@Override
			public void release() throws Exception {
				
			}
		});
		
		String json = currentMeta.toString();
		CurrentMeta de = CurrentMeta.fromJson(json);
		Assert.assertEquals(json, de.toString());
		
		Assert.assertTrue(currentMeta.hasCluster(clusterId));
		for(ShardMeta shardMeta : clusterMeta.getShards().values()){
			Assert.assertTrue(currentMeta.hasShard(clusterId, shardMeta.getId()));
		}
	}

	@Test
	public void testSetInfo(){

		//set survice keepers
		Assert.assertEquals(0, currentMeta.getSurviveKeepers(clusterId, shardId).size());
		Assert.assertEquals(null, currentMeta.getKeeperActive(clusterId, shardId));
		
		List<KeeperMeta> allKeepers = getDcKeepers(getDc(), clusterId, shardId);
		KeeperMeta active = allKeepers.get(0);
		currentMeta.setSurviveKeepers(clusterId, shardId, allKeepers, active);
		Assert.assertEquals(allKeepers.size(), currentMeta.getSurviveKeepers(clusterId, shardId).size());
		active.setActive(true);
		Assert.assertEquals(active, currentMeta.getKeeperActive(clusterId, shardId));
		
		//set keeper active
		
		KeeperMeta keeperMeta =  getDcKeepers(getDc(), clusterId, shardId).get(1);
		boolean result = currentMeta.setKeeperActive(clusterId, shardId, keeperMeta);
		Assert.assertTrue(result);
		keeperMeta.setActive(true);
		Assert.assertEquals(keeperMeta, currentMeta.getKeeperActive(clusterId, shardId));
		Assert.assertFalse(currentMeta.setKeeperActive(clusterId, shardId, keeperMeta));
		
		//set keeper active not exist
		keeperMeta.setIp(randomString(10));
		try{
			currentMeta.setKeeperActive(clusterId, shardId, keeperMeta);
			Assert.fail();
		}catch(Exception e){
			
		}
		

		Assert.assertEquals(new Pair<String, Integer>("127.0.0.1", 6379), currentMeta.getKeeperMaster(clusterId, shardId));
		Pair<String, Integer> keeperMaster = new Pair<String, Integer>("localhost", randomPort());
		currentMeta.setKeeperMaster(clusterId, shardId, keeperMaster);
		Assert.assertEquals(keeperMaster, currentMeta.getKeeperMaster(clusterId, shardId));

		
		
	}
	
	@Test
	public void testChange(){
		
		ClusterMeta future = MetaClone.clone(clusterMeta);
		String newShardId = randomString(100);
		ShardMeta shardMeta = future.getShards().remove(shardId);
		shardMeta.setId(newShardId);
		future.addShard(shardMeta);
		ClusterMetaComparator comparator = new ClusterMetaComparator(clusterMeta, future);
		comparator.compare();
		
		currentMeta.changeCluster(comparator);
		Assert.assertFalse(currentMeta.hasShard(clusterId, shardId));
		Assert.assertTrue(currentMeta.hasShard(clusterId, newShardId));
	}

	@Test
	public void testSetInfoForCRDTCluster() {

		Assert.assertEquals(0, currentMeta.getUpstreamPeerDcs(biClusterId, biShardId).size());
		Assert.assertEquals(0, currentMeta.getAllPeerMasters(biClusterId, biShardId).size());
		Assert.assertNull(currentMeta.getCurrentCRDTMaster(biClusterId, biShardId));

		// set PeerMaster
		RedisMeta redisMeta = new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L);
		currentMeta.setCurrentCRDTMaster(biClusterId, biShardId, redisMeta);
		RedisProxyMeta peerMaster =  RedisProxyMeta.create(new RedisMeta().setIp("10.0.0.2").setPort(6379).setGid(1L), null);
		currentMeta.setPeerMaster("remote-dc", biClusterId, shardId, peerMaster);
		Assert.assertEquals(1, currentMeta.getUpstreamPeerDcs(biClusterId, biShardId).size());
		Assert.assertEquals(1, currentMeta.getAllPeerMasters(biClusterId, biShardId).size());
		Assert.assertEquals(Sets.newHashSet("remote-dc"), currentMeta.getUpstreamPeerDcs(biClusterId, biShardId));
		Assert.assertEquals(new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L), currentMeta.getCurrentCRDTMaster(biClusterId, biShardId));
		Assert.assertEquals(new RedisMeta().setIp("10.0.0.2").setPort(6379).setGid(1L), currentMeta.getPeerMaster("remote-dc", biClusterId, biShardId));

		// remove PeerMaster
		currentMeta.removePeerMaster("remote-dc", biClusterId, biShardId);
		Assert.assertEquals(0, currentMeta.getUpstreamPeerDcs(biClusterId, biShardId).size());
		Assert.assertEquals(0, currentMeta.getAllPeerMasters(biClusterId, biShardId).size());
		Assert.assertEquals(new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L), currentMeta.getCurrentCRDTMaster(biClusterId, biShardId));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetPeerMasterWithErrorType() {
		currentMeta.setPeerMaster(getDc(), clusterId, shardId, (RedisProxyMeta) new RedisProxyMeta().setIp("10.0.0.1").setPort(6379).setGid(1L));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetKeeperMasterWithErrorType() {
		currentMeta.setKeeperMaster(biClusterId, biShardId, Pair.of("127.0.0.1", 6379));
	}

	@Test
	public void testGetCurrentMaster() {
		Assert.assertEquals(new RedisMeta().setIp("127.0.0.1").setPort(6379), currentMeta.getCurrentMaster(clusterId, shardId));
		Assert.assertNull(currentMeta.getCurrentMaster(biClusterId, biShardId));

		currentMeta.setCurrentCRDTMaster(biClusterId, biShardId, new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L));
		Assert.assertEquals(new RedisMeta().setIp("10.0.0.1").setPort(6379).setGid(1L), currentMeta.getCurrentMaster(biClusterId, biShardId));
	}

}
		
