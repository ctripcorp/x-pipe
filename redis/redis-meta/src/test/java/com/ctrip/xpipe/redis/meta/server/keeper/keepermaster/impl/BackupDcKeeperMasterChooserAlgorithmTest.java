package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;


import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.DcInfo;

/**
 * @author wenchao.meng
 *
 *         Nov 4, 2016
 */
public class BackupDcKeeperMasterChooserAlgorithmTest extends AbstractDcKeeperMasterChooserTest {

	private BackupDcKeeperMasterChooserAlgorithm backupAlgorithm;
	
	
	@Before
	public void beforeBackupDcKeeperMasterChooserTest() {

		backupAlgorithm = new BackupDcKeeperMasterChooserAlgorithm(clusterId, shardId, 
				dcMetaCache, currentMetaManager, metaServerConfig, metaServerMultiDcServiceManager); 
		
		when(dcMetaCache.getPrimaryDc(clusterId, shardId)).thenReturn(primaryDc);
		when(dcMetaCache.isCurrentDcPrimary(clusterId, shardId)).thenReturn(false);
		when(metaServerMultiDcServiceManager.getOrCreate(anyString())).thenReturn(metaServerMultiDcService);
	
		
		Map<String, DcInfo> dcInfos = new HashMap<>();
		dcInfos.put(primaryDc, new DcInfo("http://localhost"));
		when(metaServerConfig.getDcInofs()).thenReturn(dcInfos);
	}

	@Test
	public void testGetUpstream() throws Exception {

		backupAlgorithm.choose();
		
		verify(metaServerMultiDcService, atLeast(1)).getActiveKeeper(clusterId, shardId);
		

		logger.info("[testGetUpstream][getActiveKeeper give a result]");
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("localhost");
		keeperMeta.setPort(randomPort());
		when(metaServerMultiDcService.getActiveKeeper(clusterId, shardId)).thenReturn(keeperMeta);
		
		backupAlgorithm.choose();
				
		verify(metaServerMultiDcService, atLeast(1)).getActiveKeeper(clusterId, shardId);
	}

	@Test
	public void testVerify(){
		
		currentMetaManager.getClusterMeta(clusterId);
		currentMetaManager.getClusterMeta(clusterId);
		currentMetaManager.getClusterMeta(clusterId + "1");
		verify(currentMetaManager, times(2)).getClusterMeta(clusterId);
		verify(currentMetaManager, times(1)).getClusterMeta(clusterId + "1");
		
	}

}
