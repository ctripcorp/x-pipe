package com.ctrip.xpipe.redis.integratedtest.keeper;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.exec.ExecuteException;
import org.junit.Before;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.pool.XpipeKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class AbstractKeeperIntegratedMultiDc extends AbstractKeeperIntegrated{
	
	protected SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool = new XpipeKeyedObjectPool<>(new NettyKeyedPoolClientFactory());
	
	private KeeperMeta activeDcKeeperActive;
	
	@Before
	public void beforeAbstractKeeperIntegratedSingleDc() throws Exception{

		add(clientPool);
		
		for(DcMeta dcMeta : getDcMetas()){
			startZkServer(dcMeta.getZkServer());
		}
		
		setKeeperActive();
		startRedises();
		startKeepers();
		makeKeeperRight();
	}

	private void setKeeperActive() {
		
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
				shardMeta.setUpstream(String.format("%s:%d", activeDcKeeperActive.getIp(), activeDcKeeperActive.getPort()));
			}
		}
	}

	private void makeKeeperRight() throws InterruptedException, ExecutionException {

		logger.info(remarkableMessage("makeKeeperRight"));
		
		DcMeta dcMeta = activeDc();
		List<KeeperMeta> keepers = getDcKeepers(dcMeta.getId(), getClusterId(), getShardId());
		RedisMeta redisMaster = getRedisMaster();
		
		KeeperStateChangeJob job = new KeeperStateChangeJob(keepers, new InetSocketAddress(redisMaster.getIp(), redisMaster.getPort()), clientPool);
		job.execute().sync();
		
		for(DcMeta backupDc : backupDcs()){
			
			List<KeeperMeta> backupKeepers = getDcKeepers(backupDc.getId(), getClusterId(), getShardId());
			job = new KeeperStateChangeJob(backupKeepers, new InetSocketAddress(activeDcKeeperActive.getIp(), activeDcKeeperActive.getPort()), clientPool);
			job.execute().sync();
		}
	}

	protected void startRedises() throws ExecuteException, IOException{
		
		for(DcMeta dcMeta : getDcMetas()){
			for(RedisMeta redisMeta : getDcRedises(dcMeta.getId(), getClusterId(), getShardId())){
				startRedis(dcMeta, redisMeta);
			}
		}
	}
	
	protected void startKeepers() throws Exception{
		
		for(DcMeta dcMeta : getDcMetas()){
			
			MetaServerKeeperService metaService = createMetaService(dcMeta.getMetaServers());
			LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcMeta);
			for(KeeperMeta keeperMeta : getDcKeepers(dcMeta.getId(), getClusterId(), getShardId())){
				startKeeper(keeperMeta, metaService, leaderElectorManager);
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
