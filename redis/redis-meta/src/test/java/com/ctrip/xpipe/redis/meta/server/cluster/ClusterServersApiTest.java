package com.ctrip.xpipe.redis.meta.server.cluster;

import org.junit.Test;
import org.springframework.web.client.RestOperations;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerConsoleService;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.spring.RestTemplateFactory;

/**
 * @author wenchao.meng
 *
 * Sep 1, 2016
 */
public class ClusterServersApiTest extends AbstractMetaServerClusterTest{
	
	private int metaServerCount = 3;
	
	private int waitForMetaServerOkTime = 1500;
	
	private RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();
	
	@Test
	public void testDoChangePrimaryDc() throws Exception{

		createMetaServers(metaServerCount);
		
		sleep(waitForMetaServerOkTime);
		
		logger.info(remarkableMessage("[testDoChangePrimaryDc][begin send change primary dc message]"));
		
		for(TestMetaServer server : getServers()){
			logger.info(remarkableMessage("[testDoChangePrimaryDc][jq]"));
			MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService(server.getAddress());
			PrimaryDcChangeMessage message = consoleService.doChangePrimaryDc(getClusterId(), getShardId(), "jq");
			logger.info("{}", message);
			
			logger.info(remarkableMessage("[testDoChangePrimaryDc][oy]"));
			message = consoleService.doChangePrimaryDc(getClusterId(), getShardId(), "oy");
			logger.info("{}", message);
			break;
		}
	}
	
	@Test
	public void testClusterChanged() throws Exception{
		
		createMetaServers(metaServerCount);
		sleep(waitForMetaServerOkTime);
		logger.info(remarkableMessage("[testClusterChanged][begin send cluster change message]"));
		ClusterMeta clusterMeta = randomCluster();
		
		for(TestMetaServer server : getServers()){
			
			String path = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath(server.getAddress());
			logger.info("[testClusterChanged]{}", path);
			restTemplate.postForEntity(path, clusterMeta, String.class, clusterMeta.getId());
			restTemplate.put(path, clusterMeta, String.class, clusterMeta.getId());
			restTemplate.delete(path, clusterMeta.getId());
		}
	}

	
	@Test
	public void testUpdateUpstream() throws Exception{
		
		createMetaServers(metaServerCount);
		sleep(waitForMetaServerOkTime);
		logger.info(remarkableMessage("[testUpdateUpstream][begin send upstream update message]"));
		
		for(TestMetaServer server : getServers()){
			
			String path = META_SERVER_SERVICE.UPSTREAM_CHANGE.getRealPath(server.getAddress());
			logger.info("[testClusterChanged]{}", path);
			restTemplate.put(path, null, "cluster1", "shard1", "localhost", 7777);
		}
	}

	@Test
	public void testGetActiveKeeper() throws Exception{
		
		createMetaServers(metaServerCount);
		sleep(waitForMetaServerOkTime);
		logger.info(remarkableMessage("[testUpdateUpstream][begin send upstream update message]"));
		
		for(TestMetaServer server : getServers()){
			
			String path = META_SERVER_SERVICE.GET_ACTIVE_KEEPER.getRealPath(server.getAddress());
			logger.info("[testGetActiveKeeper]{}", path);
			KeeperMeta keeperMeta = restTemplate.getForObject(path, KeeperMeta.class, "cluster1", "shard1");
			logger.info("[testGetActiveKeeper]{}", keeperMeta);
		}
	}

	@Test
	public void testChangePrimaryDcCheck() throws Exception{

		createMetaServers(metaServerCount);
		sleep(waitForMetaServerOkTime);
		logger.info(remarkableMessage("[testChangePrimaryDcCheck][begin send primary dc check message]"));
		
		for(TestMetaServer server : getServers()){
			
			logger.info("[testChangePrimaryDcCheck]{}", server.getAddress());
			MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService(server.getAddress());
			PrimaryDcCheckMessage message = consoleService.changePrimaryDcCheck(getClusterId(), getShardId(), "newPrimaryDc");
			logger.info("[testChangePrimaryDcCheck]{}, {}", server.getAddress(), message);
		}
	}
	
	@Test
	public void testMakeMasterReadOnly() throws Exception{
		
		createMetaServers(metaServerCount);
		sleep(waitForMetaServerOkTime);
		logger.info(remarkableMessage("[testMakeMasterReadOnly][begin send make master read only message]"));
		
		for(TestMetaServer server : getServers()){
			
			logger.info("[testChangePrimaryDcCheck]{}", server.getAddress());
			MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService(server.getAddress());
			consoleService.makeMasterReadOnly(getClusterId(), getShardId(), true);
			consoleService.makeMasterReadOnly(getClusterId(), getShardId(), false);
		}
		
	}

	private ClusterMeta randomCluster() {
		
		ClusterMeta clusterMeta = new ClusterMeta();
		clusterMeta.setId(getTestName());
		return clusterMeta;
	}


}
