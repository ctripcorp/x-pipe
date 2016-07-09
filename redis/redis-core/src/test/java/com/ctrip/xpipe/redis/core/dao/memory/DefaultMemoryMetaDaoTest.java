package com.ctrip.xpipe.redis.core.dao.memory;



import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.dao.DaoException;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;



/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class DefaultMemoryMetaDaoTest extends AbstractRedisTest{
	
	private DefaultMemoryMetaDao fileDao;
	
	private String dc = "jq", clusterId = "cluster1", shardId = "shard1";

	@Before
	public void beforeDefaultFileDaoTest() throws Exception{
		
		fileDao = new DefaultMemoryMetaDao("file-dao-test.xml");
		add(fileDao);
	}
	
	@Test
	public void testUpdateKeeperActive() throws DaoException{
		
		
		List<KeeperMeta> backups = fileDao.getKeeperBackup(dc, clusterId, shardId);
		
		Assert.assertNull(fileDao.getKeeperActive(dc, clusterId, shardId));;
		
		KeeperMeta backup = backups.get(0);
		
		fileDao.updateKeeperActive(dc, clusterId, shardId, backups.get(0));
		
		KeeperMeta newActive = fileDao.getKeeperActive(dc, clusterId, shardId);
		Assert.assertEquals(backup.getIp(), newActive.getIp());
		Assert.assertEquals(backup.getPort(), newActive.getPort());
		
		
		fileDao.updateKeeperActive(dc, clusterId, shardId, new KeeperMeta());
		Assert.assertNull(fileDao.getKeeperActive(dc, clusterId, shardId));
	}

	@Test
	public void testUpdateRedisMaster() throws DaoException{
		
		Pair<String, RedisMeta> redisMaster = fileDao.getRedisMaster(clusterId, shardId);
		Assert.assertEquals(redisMaster.getKey(), "jq");
		boolean result = fileDao.updateRedisMaster(redisMaster.getKey(), clusterId, shardId, redisMaster.getValue());
		Assert.assertTrue(!result);
		
		for(RedisMeta redis : fileDao.getRedises(dc, clusterId, shardId)){
			if(!redis.equals(redisMaster.getValue())){
				result = fileDao.updateRedisMaster(redisMaster.getKey(), clusterId, shardId, redis);
				Assert.assertTrue(result);
			}
		}
	}

	@Test
	public void testUpdateUpstream() throws DaoException{
		
		String activeDc = fileDao.getActiveDc(clusterId);
		try{
			fileDao.updateUpstreamKeeper(activeDc, clusterId, shardId, "");
			Assert.fail();
		}catch(Exception e){
		}

		List<String> backDcs = fileDao.getBackupDc(clusterId);
		
		Assert.assertTrue(backDcs.size() >= 1);
		
		
		for(String dc : backDcs){
			
			String upstream = fileDao.getUpstream(dc, clusterId, shardId);
			Assert.assertNull(upstream);
			
			String address = null;
			Assert.assertFalse(fileDao.updateUpstreamKeeper(dc, clusterId, shardId, address));
			
			address = "127.0.0.1:8080";
			Assert.assertTrue(fileDao.updateUpstreamKeeper(dc, clusterId, shardId, address));
			Assert.assertFalse(fileDao.updateUpstreamKeeper(dc, clusterId, shardId, address));
		}		
		
	}

}
