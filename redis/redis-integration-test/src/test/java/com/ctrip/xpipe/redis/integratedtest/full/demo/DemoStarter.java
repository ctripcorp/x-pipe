package com.ctrip.xpipe.redis.integratedtest.full.demo;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.full.multidc.AbstractMultiDcTest;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class DemoStarter extends AbstractMultiDcTest{
	
	private String clusterId = "cluster1", shardId = "shard1";
	

	@Test
	public void startConsole() throws Exception{
	
		//clean environment
		stopXippe();
		FileUtils.forceDelete(new File(new File(getTestFileDir()).getParent()));
		FileUtils.forceMkdir(new File(getTestFileDir()));
		
		startConsoleServer();
	}
	

	@Test
	public void startMetaServer0() throws Exception{
		
		DcMeta dcMeta = getDcMeta(getAndSetDc(0));
		startZkServer(dcMeta.getZkServer());
		startMetaServers(dcMeta);		
	}

	@Test
	public void startRedises0() throws ExecuteException, IOException{
		startRedises(getAndSetDc(0), clusterId, shardId);
	}


	@Test
	public void startKeepers0() throws Exception{
		startKeepers(getAndSetDc(0), clusterId, shardId, null);
	}

	@Test
	public void startKeepers00() throws Exception{
		startKeepers(getAndSetDc(0), clusterId, shardId, 0);
	}
	
	@Test
	public void startKeepers01() throws Exception{
		startKeepers(getAndSetDc(0), clusterId, shardId, 1);
	}

	@Test
	public void stopDc0() throws ExecuteException, IOException{
		
		DcMeta dcMeta = getDcMeta(getAndSetDc(0));
		stopDc(dcMeta);
	}

	/////////////////////////////////////////DC1/////////////////////////////////////////
	@Test
	public void startMetaServer1() throws Exception{
		DcMeta dcMeta = getDcMeta(getAndSetDc(1));
		startZkServer(dcMeta.getZkServer());
		startMetaServers(dcMeta);		
	}
	

	
	@Test
	public void startRedises1() throws ExecuteException, IOException{
		startRedises(getAndSetDc(1), clusterId, shardId);
		
	}

	@Test
	public void startKeepers1() throws Exception{
		startKeepers(getAndSetDc(1), clusterId, shardId, null);
	}

	private void startRedises(String dc, String clusterId, String shardId) throws ExecuteException, IOException {
		
		DcMeta dcMeta = getDcMeta(dc);
		for(RedisMeta redisMeta : getDcMeta(dc).getClusters().get(clusterId).getShards().get(shardId).getRedises()){
			startRedis(redisMeta);
		}
	}

	private void startKeepers(String dc, String clusterId, String shardId, Integer index) throws Exception {

		DcMeta dcMeta = getDcMeta(dc);
		
		LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcMeta);
		
		int count = 0;
		for(KeeperMeta keeperMeta : getDcKeepers(dc, clusterId, shardId)){
			
			if(index == null || index == count){
				startKeeper(keeperMeta, leaderElectorManager);
			}
			count++;
		}
	}
	

	
	@Override
	protected boolean startAllDc() {
		return false;
	}
	
	@Override
	protected boolean deleteTestDirBeforeTest() {
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


	@After
	public void afterDcStarter() throws IOException{
		
		waitForAnyKeyToExit();
	}
	
}
