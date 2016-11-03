package com.ctrip.xpipe.redis.meta.server.cluster;

import org.junit.Test;
import org.springframework.web.client.RestOperations;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.redis.core.metaserver.impl.AbstractMetaService;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.spring.RestTemplateFactory;


/**
 * @author wenchao.meng
 *
 * Sep 1, 2016
 */
public class ClusterServersMulticastTest extends AbstractMetaServerClusterTest{
	
	private int metaServerCount = 3;
	private RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();
	
	@Test
	public void simpleTest(){
	}
	
	@Test
	public void testClusterChanged() throws Exception{
		
		createMetaServers(metaServerCount);
		sleep(1000);
		logger.info(remarkableMessage("[testClusterChanged][begin send cluster change message]"));
		ClusterMeta clusterMeta = randomCluster();
		
		for(TestMetaServer server : getServers()){
			String path = getChangeClusterPath(server);
			logger.info("[testClusterChanged]{}", path);
			restTemplate.postForEntity(path, clusterMeta, String.class, clusterMeta.getId());
			restTemplate.put(path, clusterMeta, String.class, clusterMeta.getId());
			restTemplate.delete(path, clusterMeta.getId());
		}
	}

	
	@Test
	public void testUpdateUpstream() throws Exception{
		
		createMetaServers(metaServerCount);
		sleep(1000);
		logger.info(remarkableMessage("[testUpdateUpstream][begin send upstream update message]"));
		
		for(TestMetaServer server : getServers()){
			String path = getUpstreamChangePath(server);
			logger.info("[testClusterChanged]{}", path);
			restTemplate.put(path, null, "cluster1", "shard1", "localhost", 7777);
		}
	}

	@Test
	public void testGetActiveKeeper() throws Exception{
		
		createMetaServers(metaServerCount);
		sleep(1000);
		logger.info(remarkableMessage("[testUpdateUpstream][begin send upstream update message]"));
		
		for(TestMetaServer server : getServers()){
			String path = getActiveKeeperPath(server);
			logger.info("[testGetActiveKeeper]{}", path);
			KeeperMeta keeperMeta = restTemplate.getForObject(path, KeeperMeta.class, "cluster1", "shard1");
			logger.info("[testGetActiveKeeper]{}", keeperMeta);
		}
	}



	private ClusterMeta randomCluster() {
		
		ClusterMeta clusterMeta = new ClusterMeta();
		clusterMeta.setId(getTestName());
		return clusterMeta;
	}

	private String getChangeClusterPath(TestMetaServer server) {

		return AbstractMetaService.getRealPath(server.getAddress(), MetaServerConsoleService.PATH_CLUSTER_CHANGE);
	}

	private String getUpstreamChangePath(TestMetaServer server) {

		return AbstractMetaService.getRealPath(server.getAddress(), MetaServerMultiDcService.PATH_UPSTREAM_CHANGE);
	}
	
	private String getActiveKeeperPath(TestMetaServer server) {
		return AbstractMetaService.getRealPath(server.getAddress(), MetaServerService.GET_ACTIVE_KEEPER);
	}

}
