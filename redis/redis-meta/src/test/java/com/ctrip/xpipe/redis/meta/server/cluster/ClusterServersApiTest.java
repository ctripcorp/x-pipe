package com.ctrip.xpipe.redis.meta.server.cluster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaserverAddress;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerConsoleService;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

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
			MetaServerConsoleService.PrimaryDcChangeRequest request = new MetaServerConsoleService.PrimaryDcChangeRequest();
			request.setMasterInfo(new MasterInfo(RunidGenerator.DEFAULT.generateRunid(), 100L));

			MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService(new MetaserverAddress("jq", server.getAddress()));
			PrimaryDcChangeMessage message = consoleService.doChangePrimaryDc(getClusterId(), getShardId(), "jq", request);
			logger.info("{}", message);
			
			logger.info(remarkableMessage("[testDoChangePrimaryDc][oy]"));
			message = consoleService.doChangePrimaryDc(getClusterId(), getShardId(), "oy", request);
			logger.info("{}", message);
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
			MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService(new MetaserverAddress("oy", server.getAddress()));
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
			MetaServerConsoleService consoleService = new DefaultMetaServerConsoleService(new MetaserverAddress("oy", server.getAddress()));
			MetaServerConsoleService.PreviousPrimaryDcMessage message = consoleService.makeMasterReadOnly(getClusterId(), getShardId(), true);
			logger.info("[testMakeMasterReadOnly][true]{}", message);
			message = consoleService.makeMasterReadOnly(getClusterId(), getShardId(), false);
			logger.info("[testMakeMasterReadOnly][false]{}", message);
		}
		
	}

	private ClusterMeta randomCluster() {
		
		ClusterMeta clusterMeta = new ClusterMeta();
		clusterMeta.setId(getTestName());
		clusterMeta.setType(ClusterType.ONE_WAY.toString());
		return clusterMeta;
	}


}
