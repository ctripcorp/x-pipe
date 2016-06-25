package com.ctrip.xpipe.redis.integratedtest.multidc.manul;

import java.io.File;
import java.io.IOException;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.multidc.AbstractMultiDcTest;
import com.ctrip.xpipe.redis.keeper.meta.MetaServiceManager;

/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class ManualStarter extends AbstractMultiDcTest{
	
	private String clusterId = "cluster1", shardId = "shard1";
	

	@Test
	public void startConsoleAndZk() throws Exception{
	
		//clean environment
		FileUtils.forceDelete(new File(getTestFileDir()));
		startZks();
//		startConsoleServer();
	}
	

	@Test
	public void startMetaServer1() throws Exception{
		startMetaServers(getDcMeta(getDc(0)));		
	}
	

	@Test
	public void startMetaServer2() throws Exception{
		startMetaServers(getDcMeta(getDc(1)));
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
		
		DcMeta dcMeta = getDcMeta(dc);
		for(RedisMeta redisMeta : getDcMeta(dc).getClusters().get(clusterId).getShards().get(shardId).getRedises()){
			startRedis(dcMeta, redisMeta);
		}
	}

	private void startKeepers(String dc, String clusterId, String shardId) throws Exception {

		DcMeta dcMeta = getDcMeta(dc);
		
		MetaServiceManager metaServiceManager = createMetaServiceManager(dcMeta.getMetaServers());
		LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcMeta);
		
		for(KeeperMeta keeperMeta : getDcKeepers(dc, clusterId, shardId)){
			startKeeper(keeperMeta, metaServiceManager, leaderElectorManager);
		}
	}
	

	
	@Override
	protected boolean startAllDc() {
		return false;
	}
	
	@Override
	protected boolean deleteTestDir() {
		return false;
	}
	
	@Override
	protected boolean stopIntegratedServers() {
		return false;
	}
	
	@Override
	protected boolean staticPort() {
		return true;
	}

	protected void startZks() {
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			startZkServer(dcMeta.getZkServer());
		}
	}



	@After
	public void afterDcStarter() throws IOException{
		
		waitForAnyKeyToExit();
	}
	
}
