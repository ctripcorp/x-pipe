package com.ctrip.xpipe.redis.integratedtest.keeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.exec.ExecuteException;
import org.junit.Before;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
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
	
	private MetaServerKeeperService metaService;
	private LeaderElectorManager leaderElectorManager;
	protected SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool = new XpipeNettyClientKeyedObjectPool();
	
	
	protected RedisMeta redisMaster;
	protected KeeperMeta activeKeeper;
	protected KeeperMeta backupKeeper;
	protected List<RedisMeta> slaves;
	
	@Before
	public void beforeAbstractKeeperIntegratedSingleDc() throws Exception{

		add(clientPool);
		
		startZkServer(getDcMeta().getZkServer());
		
		setFistKeeperActive();
		initResource();
		startRedises();
		startKeepers();
		makeKeeperRight();

		redisMaster = getRedisMaster();
		activeKeeper = getKeeperActive();
		backupKeeper = getKeepersBackup().get(0);
		slaves = getRedisSlaves();

		sleep(3000);//wait for structure to build
	}

	private void initResource() throws Exception {
		
		DcMeta dcMeta = getDcMeta();
		metaService = createMetaService(dcMeta.getMetaServers());
		leaderElectorManager = createLeaderElectorManager(dcMeta);
	}

	private void setFistKeeperActive() {	
		getDcKeepers(dc, getClusterId(), getShardId()).get(0).setActive(true);
		
	}

	private void makeKeeperRight() throws InterruptedException, ExecutionException {

		List<KeeperMeta> keepers = getDcKeepers(dc, getClusterId(), getShardId());
		
		RedisMeta redisMaster = getRedisMaster();
		
		KeeperStateChangeJob job = new KeeperStateChangeJob(keepers, new Pair<String, Integer>(redisMaster.getIp(), redisMaster.getPort()), clientPool);
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
		
		for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
			startKeeper(keeperMeta, metaService, leaderElectorManager);
		}
	}
	
	protected void startKeeper(KeeperMeta keeperMeta) throws Exception{
		startKeeper(keeperMeta, metaService, leaderElectorManager);
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
