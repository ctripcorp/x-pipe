package com.ctrip.xpipe.redis.meta.server.cluster.manul;


import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.HttpServerErrorException;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.AbstractMetaServerClusterTest;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public class MetaInfoChange extends AbstractMetaServerClusterTest{
	
	private TestMetaServer testMetaServer;
	private MetaServerConsoleService metaServerConsoleService;
	private String dc = "jq", clusterId = "cluster1", shardId = "shard1";
	
	@Before
	public void beforeClusterChange() throws Exception{
		
		createMetaServers(2);
		sleep(2000);
		testMetaServer = getServers().get(0);
		
		MetaServerConsoleServiceManager metaServerConsoleServiceManager = new DefaultMetaServerConsoleServiceManager();
		metaServerConsoleService = metaServerConsoleServiceManager.getOrCreate(String.format("http://localhost:%d", testMetaServer.getServerPort()));
	}
	
	@Test
	public void testUpstreamChange() throws IOException{
		
		try{
			metaServerConsoleService.upstreamChange(clusterId, shardId, "127.0.0.1", 6379);
		}catch(HttpServerErrorException e){
			//500 expected
		}
	}
	
	
	@Test
	public void testAddCluster() throws IOException{
		ClusterMeta clusterMeta = differentCluster(dc);
		
		metaServerConsoleService.clusterAdded(clusterId, clusterMeta);
		
		waitForAnyKeyToExit();
	}

	@Test
	public void testRemoveCluster() throws IOException{
		
		try{
			metaServerConsoleService.clusterDeleted(clusterId);;
		}catch(Exception e){
			logger.error("[testRemoveCluster]", e);
		}
		waitForAnyKeyToExit();
	}

	@Test
	public void testChangeClusterShard() throws IOException{
		
		//change keeper
		try{
			ClusterMeta clusterMeta = getCluster(dc, clusterId);
			ShardMeta shardMeta = clusterMeta.getShards().get(shardId);
			shardMeta.setId(randomString(10));
			changeClusterKeeper(clusterMeta);
			
			clusterMeta.removeShard(shardId);
			clusterMeta.addShard(shardMeta);
			metaServerConsoleService.clusterModified(clusterMeta.getId(), clusterMeta);;
		}catch(Exception e){
			logger.error("[testChangeClusterKeeper]", e);
		}
		
		waitForAnyKeyToExit();
	}

	@Test
	public void testChangeClusterKeeper() throws IOException{
		
		sleep(2000);
		//change keeper
		try{
			ClusterMeta clusterMeta = getCluster(dc, clusterId);
			changeClusterKeeper(clusterMeta);
			metaServerConsoleService.clusterModified(clusterMeta.getId(), clusterMeta);;
		}catch(Exception e){
			logger.error("[testChangeClusterKeeper]", e);
		}
		
		waitForAnyKeyToExit();
	}


	@Override
	protected String getXpipeMetaConfigFile() {
		return TestMetaServer.DEFAULT_CONFIG_FILE;
	}
}
