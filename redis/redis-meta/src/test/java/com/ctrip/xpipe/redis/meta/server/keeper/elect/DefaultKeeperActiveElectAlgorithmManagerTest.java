package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;

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
	@Mock
	private KeeperElectReElectService keeperElectReElectService;

	private UnitTestServerConfig config;

	private Long clusterDbId, shardDbId;
	
	@Before
	public void beforeDefaultKeeperActiveElectAlgorithmManagerTest(){
		clusterDbId = getClusterDbId();
		shardDbId = getShardDbId();
		config = new UnitTestServerConfig();
		kaem = new DefaultKeeperActiveElectAlgorithmManager();
		kaem.setDcMetaCache(dcMetaCache);
		kaem.setMetaServerConfig(config);
		kaem.setKeeperElectReElectService(keeperElectReElectService);
	}
	
	@Test
	public void testGetAlgorithm() {
		Assert.assertTrue(kaem.get(clusterDbId, shardDbId) instanceof StrategyAwareKeeperActiveElectAlgorithm);
	}

	@Test
	public void testStrategyChangeTriggersReElect() {
		kaem.registerStrategyListener();
		config.onChange("keeper.elect.strategy", "AUTO", "BM_PREFER");
		verify(keeperElectReElectService).reElectAll();
	}
}
