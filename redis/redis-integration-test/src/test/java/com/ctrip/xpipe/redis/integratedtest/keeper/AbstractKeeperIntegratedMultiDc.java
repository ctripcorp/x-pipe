package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.tuple.Pair;
import org.apache.commons.exec.ExecuteException;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class AbstractKeeperIntegratedMultiDc extends AbstractKeeperIntegrated{
	
	
	protected KeeperMeta activeDcKeeperActive;

	protected Map<String, LeaderElectorManager> leaderElectorManagers = new HashMap<>();
	
	@Before
	public void beforeAbstractKeeperIntegratedSingleDc() throws Exception{
		
		for(DcMeta dcMeta : getDcMetas()){
			startZkServer(dcMeta.getZkServer());
		}
		
		setKeeperActive();
		startRedises();
		startKeepers();
		makeKeeperRight();
		
		sleep(3000);
	}

	protected void setKeeperActive() {
		
		for(DcMeta dcMeta : getDcMetas()){
			ClusterMeta clusterMeta = dcMeta.getClusters().get(getClusterId());
			if(clusterMeta.getActiveDc().equals(dcMeta.getId())){
				activeDcKeeperActive = getDcKeepers(dcMeta.getId(), getClusterId(), getShardId()).get(0);
				activeDcKeeperActive.setActive(true);
			}
		}

		for(DcMeta dcMeta : getDcMetas()){
			ClusterMeta clusterMeta = dcMeta.getClusters().get(getClusterId());
			if(!clusterMeta.getActiveDc().equals(dcMeta.getId())){
				ShardMeta shardMeta = clusterMeta.getShards().get(getShardId());
				shardMeta.getKeepers().get(0).setActive(true);
			}
		}
	}
	
	protected String getPrimaryDc(){
		
		for(DcMeta dcMeta : getDcMetas()){
			
			ClusterMeta clusterMeta = dcMeta.getClusters().get(getClusterId());
			return clusterMeta.getActiveDc().trim().toLowerCase();
		}
		return null;
	}
	
	protected String getBackupDc(){
		
		for(DcMeta dcMeta : getDcMetas()){
			ClusterMeta clusterMeta = dcMeta.getClusters().get(getClusterId());
			return clusterMeta.getBackupDcs().trim().toLowerCase();
		}
		return null;
	}


	private void makeKeeperRight() throws Exception {

		logger.info(remarkableMessage("makeKeeperRight"));
		
		DcMeta dcMeta = activeDc();
		List<KeeperMeta> keepers = getDcKeepers(dcMeta.getId(), getClusterId(), getShardId());
		RedisMeta redisMaster = getRedisMaster();
		
		KeeperStateChangeJob job = new KeeperStateChangeJob(keepers, new Pair<String, Integer>(redisMaster.getIp(), redisMaster.getPort()), null, getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
		job.execute().sync();
		
		for(DcMeta backupDc : backupDcs()){
			
			List<KeeperMeta> backupKeepers = getDcKeepers(backupDc.getId(), getClusterId(), getShardId());
			job = new KeeperStateChangeJob(backupKeepers, new Pair<String, Integer>(activeDcKeeperActive.getIp(), activeDcKeeperActive.getPort()), null, getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
			job.execute().sync();
		}
	}

	protected void makeBackupDcKeeperRight(String dc) throws Exception {
		for(DcMeta backupDc : backupDcs()){
			if (!backupDc.getId().equalsIgnoreCase(dc)) continue;

			List<KeeperMeta> backupKeepers = getDcKeepers(backupDc.getId(), getClusterId(), getShardId());
			KeeperStateChangeJob job = new KeeperStateChangeJob(backupKeepers, new Pair<String, Integer>(
					activeDcKeeperActive.getIp(), activeDcKeeperActive.getPort()), null,
					getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
			job.execute().sync();
		}
	}

	protected void makePrimaryDcKeeperRight() throws Exception {
		makePrimaryDcKeeperRight(getRedisMaster());
	}

	protected void makePrimaryDcKeeperRight(RedisMeta redisMaster) throws Exception {
		List<KeeperMeta> keepers = getDcKeepers(getPrimaryDc(), getClusterId(), getShardId());
		KeeperStateChangeJob job = new KeeperStateChangeJob(keepers, new Pair<String, Integer>(redisMaster.getIp(), redisMaster.getPort()), null, getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
		job.execute().sync();
	}

	protected void startRedises() throws ExecuteException, IOException{
		
		for(DcMeta dcMeta : getDcMetas()){
			for(RedisMeta redisMeta : getDcRedises(dcMeta.getId(), getClusterId(), getShardId())){
				startRedis(redisMeta);
			}
		}
	}
	
	protected void startKeepers() throws Exception{
		
		for(DcMeta dcMeta : getDcMetas()){
			
			LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcMeta);
			leaderElectorManagers.put(dcMeta.getId(), leaderElectorManager);
			for(KeeperMeta keeperMeta : getDcKeepers(dcMeta.getId(), getClusterId(), getShardId())){
				startKeeper(keeperMeta, leaderElectorManager);
			}
		}
		
	}
	
	@Override
	protected void endPrepareRedisConfig(RedisMeta redisMeta, StringBuilder sb) {
		
		if(redisMeta.isMaster()){
			return;
		}
		KeeperMeta activeKeeper = getKeeperActive(redisMeta);
		logger.info("redis({}:{}) master({}:{})", redisMeta.getIp(), redisMeta.getPort(), activeKeeper.getIp(), activeKeeper.getPort());
		sb.append(String.format("slaveof %s %d\r\n", activeKeeper.getIp(), activeKeeper.getPort()));
	}
	
	@Override
	protected List<RedisMeta> getRedisSlaves() {
		return getAllRedisSlaves();
	}
}
