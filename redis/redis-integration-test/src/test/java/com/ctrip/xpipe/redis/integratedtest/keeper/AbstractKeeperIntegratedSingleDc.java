package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.tuple.Pair;
import org.apache.commons.exec.ExecuteException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class AbstractKeeperIntegratedSingleDc extends AbstractKeeperIntegrated{
	
	protected String dc = "jq";
	
	private LeaderElectorManager leaderElectorManager;
	
	
	protected RedisMeta redisMaster;
	protected KeeperMeta activeKeeper;
	protected KeeperMeta backupKeeper;
	protected List<RedisMeta> slaves;
	
	@Before
	public void beforeAbstractKeeperIntegratedSingleDc() throws Exception{

		if(startServers()){
			startZkServer(getDcMeta().getZkServer());
		}

		setFistKeeperActive();
		initResource();

		if(startServers()){
			startRedises();
			startKeepers();
			makeKeeperRight();
		}

		redisMaster = getRedisMaster();
		activeKeeper = getKeeperActive();
		List<KeeperMeta> keepersBackup = getKeepersBackup();
		if(keepersBackup.size() > 0){
			backupKeeper = getKeepersBackup().get(0);
		}
		slaves = getRedisSlaves();

		sleep(getInitSleepMilli());//wait for structure to build
	}

	@After
	public void relaseServers() {
		super.afterAbstractIntegratedTest();
	}

	protected boolean startServers() {
		return true;
	}

	protected int getInitSleepMilli() {
		return 3000;
	}

	private void initResource() throws Exception {
		
		DcMeta dcMeta = getDcMeta();
		leaderElectorManager = createLeaderElectorManager(dcMeta);
	}

	private void setFistKeeperActive() {	
		getDcKeepers(dc, getClusterId(), getShardId()).get(0).setActive(true);
		
	}

	protected void makeKeeperRight() throws Exception {

		List<KeeperMeta> keepers = getDcKeepers(dc, getClusterId(), getShardId());
		
		RedisMeta redisMaster = getRedisMaster();
		
		KeeperStateChangeJob job = new KeeperStateChangeJob(keepers, new Pair<String, Integer>(redisMaster.getIp(), redisMaster.getPort()), null, getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
		job.execute().sync();
	}

	protected void startRedises() throws ExecuteException, IOException{
		
		for(RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())){
			startRedis(redisMeta);
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
			startKeeper(keeperMeta, leaderElectorManager);
		}
	}
	
	protected RedisKeeperServer startKeeper(KeeperMeta keeperMeta) throws Exception{
		return startKeeper(keeperMeta, leaderElectorManager);
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
