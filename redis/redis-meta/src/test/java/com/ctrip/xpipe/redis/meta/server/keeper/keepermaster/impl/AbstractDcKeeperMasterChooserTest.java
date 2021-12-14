package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;


import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author wenchao.meng
 *
 * Nov 13, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractDcKeeperMasterChooserTest extends AbstractMetaServerTest{
	
	protected int checkIntervalSeconds = 1;
	
	protected String primaryDc = "jq";
	
	protected String clusterId = "cluster1";
	
	protected String shardId = "shard1";

	protected Long clusterDbId = 1L;

	protected Long shardDbId = 1L;
	
	@Mock
	protected DcMetaCache dcMetaCache;
	
	@Mock
	protected CurrentMetaManager currentMetaManager;

	@Mock
	protected MultiDcService multiDcService;

	@Before
	public void beforeAbstractDcKeeperMasterChooserTest(){
		
	}


}
