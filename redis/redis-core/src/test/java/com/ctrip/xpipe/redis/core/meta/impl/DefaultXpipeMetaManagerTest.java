package com.ctrip.xpipe.redis.core.meta.impl;

import java.net.InetSocketAddress;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.utils.IpUtils;
/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class DefaultXpipeMetaManagerTest extends AbstractRedisTest{
	
	private DefaultXpipeMetaManager metaManager;
	
	private String dc = "jq", clusterId = "cluster1", shardId = "shard1";
	private String dcBak = "fq";

	@Before
	public void beforeDefaultFileDaoTest() throws Exception{
		
		metaManager = (DefaultXpipeMetaManager) DefaultXpipeMetaManager.buildFromFile("file-dao-test.xml");
		add(metaManager);
	}
	
	@Test
	public void testupdateUpstream() throws Exception{
		
		String ip = "localhost";
		int port = randomPort();
		
		try{
			metaManager.updateUpstream(dc, clusterId, shardId, ip, port);
			Assert.fail();
		}catch(Exception e){
			
		}

		String upstream = metaManager.getUpstream(dcBak, clusterId, shardId);
		InetSocketAddress address = IpUtils.parseSingle(upstream);
		metaManager.updateUpstream(dcBak, clusterId, shardId, address.getAddress().getHostName(), address.getPort() + 1);
		String newUpstream = metaManager.getUpstream(dcBak, clusterId, shardId);
		logger.info("[testupdateUpstream]{}", newUpstream);
		Assert.assertNotEquals(upstream, newUpstream);
	}
	
	@Test
	public void testHas(){
		
		Assert.assertTrue(metaManager.hasCluster(dc, clusterId));;
		Assert.assertFalse(metaManager.hasCluster(dc, randomString()));;
		Assert.assertFalse(metaManager.hasCluster(randomString(), clusterId));;

		Assert.assertTrue(metaManager.hasShard(dc, clusterId, shardId));;
		Assert.assertFalse(metaManager.hasShard(dc, clusterId, randomString()));;
		Assert.assertFalse(metaManager.hasShard(dc, randomString(), shardId));;
		Assert.assertFalse(metaManager.hasShard(randomString(), clusterId, shardId));;

		
		
}
	
	@Test
	public void testSetKeeperAlive(){
		
		List<KeeperMeta> allSurvice = metaManager.getAllSurviceKeepers(dc, clusterId, shardId);
		logger.info("[testSetKeeperAlive][allAlive]{}", allSurvice);
		Assert.assertEquals(0, allSurvice.size());
		
		List<KeeperMeta> allKeepers = metaManager.getKeepers(dc, clusterId, shardId);
		for(KeeperMeta allOne : allKeepers){
			allOne.setSurvive(true);
		}

		allSurvice = metaManager.getAllSurviceKeepers(dc, clusterId, shardId);
		Assert.assertEquals(0, allSurvice.size());

		metaManager.setSurviveKeepers(dc, clusterId, shardId, allKeepers);

		
		allSurvice = metaManager.getAllSurviceKeepers(dc, clusterId, shardId);
		Assert.assertEquals(allKeepers.size(), allSurvice.size());
		

		try{
			KeeperMeta nonExist = createNonExistKeeper(allKeepers);
			allKeepers.add(nonExist);
			metaManager.setSurviveKeepers(dc, clusterId, shardId, allKeepers);
			Assert.fail();
		}catch(IllegalArgumentException e){
			
		}
		
	}
	
	@Test
	public void testUpdateKeeperActive() throws MetaException{
		
		
		List<KeeperMeta> backups = metaManager.getKeeperBackup(dc, clusterId, shardId);
		
		Assert.assertNotNull(metaManager.getKeeperActive(dc, clusterId, shardId));;
		
		KeeperMeta backup = backups.get(0);
		
		metaManager.updateKeeperActive(dc, clusterId, shardId, backups.get(0));
		
		KeeperMeta newActive = metaManager.getKeeperActive(dc, clusterId, shardId);
		Assert.assertEquals(backup.getIp(), newActive.getIp());
		Assert.assertEquals(backup.getPort(), newActive.getPort());
		
		
		metaManager.updateKeeperActive(dc, clusterId, shardId, new KeeperMeta());
		Assert.assertNull(metaManager.getKeeperActive(dc, clusterId, shardId));
	}

	@Test
	public void testUpdateRedisMaster() throws MetaException{
		
		Pair<String, RedisMeta> redisMaster = metaManager.getRedisMaster(clusterId, shardId);
		Assert.assertEquals(redisMaster.getKey(), "jq");
		boolean result = metaManager.updateRedisMaster(redisMaster.getKey(), clusterId, shardId, redisMaster.getValue());
		Assert.assertTrue(!result);

		KeeperMeta activeKeeper = null;
		for(KeeperMeta keeperMeta : metaManager.getKeepers(dc, clusterId, shardId)){
			if(keeperMeta.getMaster().equals(String.format("%s:%d", redisMaster.getValue().getIp(), redisMaster.getValue().getPort()))){
				activeKeeper = keeperMeta;
			}
		}
		Assert.assertNotNull(activeKeeper);

		for(RedisMeta redis : metaManager.getRedises(dc, clusterId, shardId)){
			
			if(!redis.equals(redisMaster.getValue())){
				String master = String.format("%s:%d", redis.getIp(), redis.getPort());
				Assert.assertNotEquals(activeKeeper.getMaster(), master);
				result = metaManager.updateRedisMaster(redisMaster.getKey(), clusterId, shardId, redis);
				Assert.assertTrue(result);
				
				KeeperMeta active = metaManager.getKeeperActive(redisMaster.getKey(), clusterId, shardId);
				Assert.assertEquals(active.getMaster(), master);
			}
		}
	}
}
