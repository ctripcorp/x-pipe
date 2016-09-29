package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

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
	
	@Before
	public void beforeDefaultKeeperActiveElectAlgorithmManagerTest(){
		
		clusterId = getClusterId();
		shardId = getShardId();
		kaem = new DefaultKeeperActiveElectAlgorithmManager();
		kaem.setDcMetaCache(dcMetaCache);
	}
	
	@Test
	public void testActive(){
		
		when(dcMetaCache.isActiveDc(getClusterId(), getShardId())).thenReturn(true);
		

		Assert.assertTrue(kaem.get(clusterId, shardId) instanceof DefaultKeeperActiveElectAlgorithm);;
	}
	
	@Test
	public void testBackup(){
		
		when(dcMetaCache.isActiveDc(getClusterId(), getShardId())).thenReturn(false);
		Assert.assertTrue(kaem.get(clusterId, shardId) instanceof UserDefinedPriorityKeeperActiveElectAlgorithm);
		
	}


}
