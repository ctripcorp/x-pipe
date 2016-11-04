package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 *         Nov 4, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class BackupDcKeeperMasterChooserTest extends AbstractMetaServerTest {

	private BackupDcKeeperMasterChooser backupDcKeeperMasterChooser;
	
	private int checkIntervalSeconds = 1;
	
	private String primaryDc = "jq";
	
	private String clusterId = "cluster1";
	private String shardId = "shard1";
	
	@Mock
	private MetaServerConfig metaServerConfig; 

	@Mock
	private MetaServerMultiDcServiceManager metaServerMultiDcServiceManager; 

	@Mock
	private MetaServerMultiDcService metaServerMultiDcService;

	@Mock
	private DcMetaCache dcMetaCache;
	
	@Mock
	private CurrentMetaManager currentMetaManager;
	
	@Before
	public void beforeBackupDcKeeperMasterChooserTest() {

		backupDcKeeperMasterChooser = new BackupDcKeeperMasterChooser(clusterId, shardId, metaServerConfig,
				metaServerMultiDcServiceManager, dcMetaCache, currentMetaManager, scheduler, checkIntervalSeconds);
		
		when(dcMetaCache.getPrimaryDc(clusterId, shardId)).thenReturn(primaryDc);
		when(dcMetaCache.isCurrentDcPrimary(clusterId, shardId)).thenReturn(false);
		when(metaServerMultiDcServiceManager.getOrCreate(anyString())).thenReturn(metaServerMultiDcService);
	
		
		Map<String, DcInfo> dcInfos = new HashMap<>();
		dcInfos.put(primaryDc, new DcInfo("http://localhost"));
		when(metaServerConfig.getDcInofs()).thenReturn(dcInfos);
	}

	@Test
	public void testGetUpstream() throws Exception {

		backupDcKeeperMasterChooser.start();
		
		sleep(checkIntervalSeconds * 1000);
		verify(metaServerMultiDcService, atLeast(1)).getActiveKeeper(clusterId, shardId);
		

		logger.info("[testGetUpstream][getActiveKeeper give a result]");
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("localhost");
		keeperMeta.setPort(randomPort());
		when(metaServerMultiDcService.getActiveKeeper(clusterId, shardId)).thenReturn(keeperMeta);
		
		sleep(checkIntervalSeconds * 1500);
		
		verify(currentMetaManager, atLeast(1)).setKeeperMaster(clusterId, shardId, keeperMeta.getIp(), keeperMeta.getPort());
		
		
		verify(metaServerMultiDcService, atLeast(1)).getActiveKeeper(clusterId, shardId);
		backupDcKeeperMasterChooser.stop();
		sleep(checkIntervalSeconds * 1000);
		verifyNoMoreInteractions(metaServerMultiDcService);
	}

	@Test
	public void testGetUpstreamRelease() throws Exception {

		backupDcKeeperMasterChooser.start();
		
		sleep(checkIntervalSeconds * 1000);

		verify(metaServerMultiDcService, atLeast(1)).getActiveKeeper(clusterId, shardId);
		
		backupDcKeeperMasterChooser.release();
		verifyNoMoreInteractions(metaServerMultiDcService);
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
