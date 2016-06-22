package com.ctrip.xpipe.redis.integratedtest.multidc.manul;

import java.io.IOException;

import org.apache.commons.exec.ExecuteException;
import org.junit.After;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.multidc.AbstractMultiDcTest;
import com.ctrip.xpipe.redis.keeper.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.keeper.meta.MetaServiceManager;

/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class DcStarter extends AbstractMultiDcTest{
	
	private String clusterId = "cluster1", shardId = "shard1";
	

	@Test
	public void startConsole(){
		
	}
	

	@Test
	public void startMetaServer1() throws Exception{
		startMetaServer(getDc(0));		
	}
	

	@Test
	public void startMetaServer2() throws Exception{
		startMetaServer(getDc(1));
	}
	
	@Test
	public void startKeepers1() throws Exception{
		startKeepers(getDc(0), clusterId, shardId);
	}

	@Test
	public void startKeepers2() throws Exception{
		startKeepers(getDc(1), clusterId, shardId);
	}
	
	@Test
	public void startRedises1() throws ExecuteException, IOException{
		startRedises(getDc(0), clusterId, shardId);
	}

	@Test
	public void startRedises2() throws ExecuteException, IOException{
		startRedises(getDc(1), clusterId, shardId);
		
	}

	private void startRedises(String dc, String clusterId, String shardId) throws ExecuteException, IOException {
		
		DcInfo dcInfo = getDcInfos().get(dc);
		for(RedisMeta redisMeta : getDcMeta(dc).getClusters().get(clusterId).getShards().get(shardId).getRedises()){
			startRedis(dcInfo, redisMeta);
		}
	}

	private void startKeepers(String dc, String clusterId, String shardId) throws Exception {

		DcInfo dcInfo = getDcInfos().get(dc);
		
		MetaServiceManager metaServiceManager = createMetaServiceManager(dcInfo);
		LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcInfo);
		
		for(KeeperMeta keeperMeta : getDcKeepers(dc, clusterId, shardId)){
			startKeeper(dcInfo, keeperMeta, metaServiceManager, leaderElectorManager);
		}
	}

	private void startMetaServer(String dcName) throws Exception {
		
		DcMeta dcMeta = getDcMeta(dcName);
		DcInfo dcInfo = getDcInfos().get(dcName);
		startMetaServer(dcMeta, dcInfo);
	}
	

	
	@Override
	protected boolean startAllDc() {
		return false;
	}
	
	@Override
	protected boolean deleteTestDir() {
		return false;
	}

	@After
	public void afterDcStarter() throws IOException{
		
		waitForAnyKeyToExit();
	}
	
}
