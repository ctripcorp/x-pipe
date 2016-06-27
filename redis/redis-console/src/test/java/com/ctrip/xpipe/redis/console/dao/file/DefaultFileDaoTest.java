package com.ctrip.xpipe.redis.console.dao.file;


import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.dao.DaoException;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class DefaultFileDaoTest extends AbstractConsoleTest{
	
	private DefaultFileDao fileDao;
	
	@Before
	public void beforeDefaultFileDaoTest(){
		fileDao = new DefaultFileDao("file-dao-test.xml");
	}
	
	@Test
	public void testUpdateKeeperActive() throws DaoException{
		
		
		String dc = "jq", clusterId = "cluster1", shardId = "shard1";
		
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
		
		String dc = "jq", clusterId = "cluster1", shardId = "shard1";
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
	

}
