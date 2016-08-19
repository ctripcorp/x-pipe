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
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class AbstractKeeperIntegratedSingleDc extends AbstractKeeperIntegrated{
	
	protected String dc = "jq";
	
	protected SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool = new XpipeKeyedObjectPool<>(new NettyKeyedPoolClientFactory());
	
	@Before
	public void beforeAbstractKeeperIntegratedSingleDc() throws Exception{

		add(clientPool);
		
		startZkServer(getDcMeta().getZkServer());
		
		setFistKeeperActive();
		startRedises();
		startKeepers();
		makeKeeperRight();
		sleep(3000);//wait for structure to build
	}

	private void setFistKeeperActive() {	
		getDcKeepers(dc, getClusterId(), getShardId()).get(0).setActive(true);
		
	}

	private void makeKeeperRight() throws InterruptedException, ExecutionException {

		List<KeeperMeta> keepers = getDcKeepers(dc, getClusterId(), getShardId());
		
		RedisMeta redisMaster = getRedisMaster();
		
		KeeperStateChangeJob job = new KeeperStateChangeJob(keepers, new InetSocketAddress(redisMaster.getIp(), redisMaster.getPort()), clientPool);
		job.execute().sync();
	}

	protected void startRedises() throws ExecuteException, IOException{
		
		for(RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())){
			startRedis(getDcMeta(), redisMeta);
		}
	}
	
	protected DcMeta getDcMeta() {
		return getDcMeta(dc);
	}

	protected KeeperMeta getKeeperActive(){
		return getKeeperActive(dc);
	}

	protected List<KeeperMeta> getKeepersBackup() {
		return getKeepersBackup(dc);
	}
	
	protected void startKeepers() throws Exception{
		
		DcMeta dcMeta = getDcMeta();
		MetaServerKeeperService metaService = createMetaService(dcMeta.getMetaServers());
		LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcMeta);
		
		for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
			startKeeper(keeperMeta, metaService, leaderElectorManager);
		}
		
	}
	
	@Override
	protected void endPrepareRedisConfig(RedisMeta redisMeta, StringBuilder sb) {
		
		if(redisMeta.isMaster()){
			return;
		}
		KeeperMeta activeKeeper = getKeeperActive();
		sb.append(String.format("slaveof %s %d\r\n", activeKeeper.getIp(), activeKeeper.getPort()));
	}
	
	@Override
	protected List<RedisMeta> getRedisSlaves() {
		return getRedisSlaves(dc);
	}
	
	
}
