package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

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

	
	@Mock
	protected MetaServerMultiDcService metaServerMultiDcService;

	@Mock
	protected DcMetaCache dcMetaCache;
	
	@Mock
	protected CurrentMetaManager currentMetaManager;
	
	
	@Before
	public void beforeAbstractDcKeeperMasterChooserTest(){
		
	}


}
