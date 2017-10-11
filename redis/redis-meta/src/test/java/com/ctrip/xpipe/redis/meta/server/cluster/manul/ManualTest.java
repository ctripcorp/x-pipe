package com.ctrip.xpipe.redis.meta.server.cluster.manul;


import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.AbstractMetaServerClusterTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskExecutor;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Aug 3, 2016
 */
public class ManualTest extends AbstractMetaServerClusterTest {

	@Before
	public void beforeManualTest() {
		System.setProperty(ArrangeTaskExecutor.ARRANGE_TASK_EXECUTOR_START, "true");

	}

	
	@Test
	public void startServersJq() throws Exception {

		createMetaServers(1);

		waitForAnyKeyToExit();
	}

	@Test
	public void startServersOy() throws Exception {
		
		System.setProperty(TestMetaServer.KEY_CONFIG_FILE, "metaserver--oy.xml");
		DefaultFoundationService.setDataCenter("oy");
		createMetaServers(1);

		waitForAnyKeyToExit();
	}

	@Test
	public void testMetaServerGc() throws Exception {

		for (int i = 0; i < 10000; i++) {

			try{
				TestMetaServer server = createMetaServer(i, 9747, getZkPort());
				server.stop();
				server.dispose();
			}catch(Exception e){
				logger.error("[testMetaServer]", e);
			}
		}

	}

}
