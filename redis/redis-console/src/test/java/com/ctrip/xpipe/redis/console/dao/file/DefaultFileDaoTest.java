package com.ctrip.xpipe.redis.console.dao.file;


import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

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
	public void testUpdateKeeperActive(){
		
		
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
	
	

}
