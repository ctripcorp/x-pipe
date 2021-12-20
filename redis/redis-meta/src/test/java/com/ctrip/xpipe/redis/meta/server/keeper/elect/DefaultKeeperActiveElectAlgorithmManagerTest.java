package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Sep 9, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperActiveElectAlgorithmManagerTest extends AbstractMetaServerTest{
	
	private DefaultKeeperActiveElectAlgorithmManager kaem;
	@Mock
	private DcMetaCache dcMetaCache;
	
	private String clusterId, shardId;

	private Long clusterDbId, shardDbId;
	
	@Before
	public void beforeDefaultKeeperActiveElectAlgorithmManagerTest(){
		
		clusterId = getClusterId();
		shardId = getShardId();
		clusterDbId = getClusterDbId();
		shardDbId = getShardDbId();
		kaem = new DefaultKeeperActiveElectAlgorithmManager();
		kaem.setDcMetaCache(dcMetaCache);
	}
	
	@Test
	public void testActive(){
		
		when(dcMetaCache.isCurrentDcPrimary(getClusterDbId(), getShardDbId())).thenReturn(true);
		

		Assert.assertTrue(kaem.get(clusterDbId, shardDbId) instanceof DefaultKeeperActiveElectAlgorithm);
	}
	
	@Test
	public void testBackup(){
		
		when(dcMetaCache.isCurrentDcPrimary(getClusterDbId(), getShardDbId())).thenReturn(false);
		Assert.assertTrue(kaem.get(clusterDbId, shardDbId) instanceof DefaultKeeperActiveElectAlgorithm);
		
	}


}
