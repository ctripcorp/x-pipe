package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.*;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public class AbstractComparatorTest extends AbstractRedisTest{
	
	private String comparatorFile = "comparator.xml";
	private String []dcs = new String[]{"jq", "fq"};

	private String clusterId = "cluster1", shardId = "shard1";
	
	protected String getXpipeMetaConfigFile() {
		return comparatorFile;
	}

	protected DcMeta getDc() {
		return super.getDcMeta(dcs[0]);
	}

	
	protected ClusterMeta getCluster() {
		return super.getCluster(dcs[0], clusterId);
	}

	protected ShardMeta getShard() {
		return super.getShard(dcs[0], clusterId, shardId);
	}
	

	protected KeeperMeta differentKeeper(ShardMeta current) {
		
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("localhost");
		keeperMeta.setPort(maxPort(current) + 1);
		return keeperMeta;
	}

	protected ApplierMeta differentApplier(ShardMeta current) {

		ApplierMeta applierMeta = new ApplierMeta();
		applierMeta.setIp("localhost");
		applierMeta.setPort(maxPort(current) + 1);
		return applierMeta;
	}

	protected ShardMeta differentShard(ClusterMeta current) {
		
		ShardMeta shardMeta = new ShardMeta();
		shardMeta.setId(randomString());
		shardMeta.setDbId(Math.abs(randomLong()));
		return shardMeta;
	}

	protected SourceMeta differentSourceShard() {
		return new SourceMeta();
	}

	private int maxPort(ShardMeta current) {
		int maxPort = Integer.MIN_VALUE;
		for(RedisMeta redis : current.getRedises()){
			if(redis.getPort() > maxPort){
				maxPort = redis.getPort();
			}
		}

		for(KeeperMeta keeper : current.getKeepers()){
			if(keeper.getPort() > maxPort){
				maxPort = keeper.getPort();
			}
		}

		for(ApplierMeta applier : current.getAppliers()){
			if(applier.getPort() > maxPort){
				maxPort = applier.getPort();
			}
		}

		return maxPort;
	}
}
