package com.ctrip.xpipe.redis.meta.server;


import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.SourceMeta;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.testutils.MemoryPrinter;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class AbstractMetaServerTest extends AbstractRedisTest{
	
	private String xpipeConfig = "meta-test.xml";
	private String dc = "jq", clusterId = "cluster1", shardId = "shard1";
	private Long clusterDbId = 1L, shardDbId = 1L;
	
	protected MetaServerConfig  config = new DefaultMetaServerConfig();
	
	private MemoryPrinter memoryPrinter;
	
	@Before
	public void beforeAbstractMetaServerTest() throws Exception{

		memoryPrinter = new MemoryPrinter(scheduled, 500);
		memoryPrinter.start();
	}

	@After
	public void afterAbstractMetaServerTest() throws Exception{
		memoryPrinter.stop();
	}

	
	@Override
	protected String getXpipeMetaConfigFile() {
		return xpipeConfig;
	}

	
	public String getDc() {
		return dc;
	}
	
	public String [] getDcs(){
		return new String[]{"jq", "fq"};
	}
	
	public String getClusterId() {
		return clusterId;
	}
	
	public String getShardId() {
		return shardId;
	}

	public Long getClusterDbId() {
		return clusterDbId;
	}

	public Long getShardDbId() {
		return shardDbId;
	}

	public void exchangeClusterShards(ClusterMeta cluster1, ClusterMeta cluster2) {
		Map<String, ShardMeta> shards1 = new HashMap<>(cluster1.getShards());
		Map<String, ShardMeta> shards2 = new HashMap<>(cluster2.getShards());
		cluster1.getShards().clear();
		cluster2.getShards().clear();
		shards1.values().forEach(cluster2::addShard);
		shards2.values().forEach(cluster1::addShard);
	}

	public void exchangeClusterSources(ClusterMeta cluster1, ClusterMeta cluster2) {
		List<SourceMeta> sources1 = new ArrayList<>(cluster1.getSources());
		List<SourceMeta> sources2 = new ArrayList<>(cluster2.getSources());
		cluster1.getSources().clear();
		cluster2.getSources().clear();
		sources1.forEach(cluster2::addSource);
		sources2.forEach(cluster1::addSource);
	}

}
