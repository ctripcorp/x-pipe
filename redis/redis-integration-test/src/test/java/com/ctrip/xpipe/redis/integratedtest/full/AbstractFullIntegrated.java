package com.ctrip.xpipe.redis.integratedtest.full;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
import com.ctrip.xpipe.redis.integratedtest.ConsoleStart;
import com.ctrip.xpipe.redis.integratedtest.DcInfo;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.meta.server.TestMetaServer;
import com.ctrip.xpipe.utils.IpUtils;
import org.apache.commons.exec.ExecuteException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public abstract class AbstractFullIntegrated extends AbstractIntegratedTest{

	private Map<String, DcInfo>  dcs = new ConcurrentHashMap<>();
	
	private int consolePort = 8080;
	

	@Before
	public void beforeAbstractIntegratedTest() throws Exception {
		
		createDcs();

		if(!stopIntegratedServers()){
			return;
		}

		stopXippe();
	}

	protected void stopXippe() throws ExecuteException, IOException {
		
		stopConsole();
		//stop all servers
		stopAllRedisServer();
				
		// stop keeper
		for (DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
			stopDc(dcMeta);
		}
	}

	protected void stopDc(DcMeta dcMeta) throws ExecuteException, IOException {
		
		for(InetSocketAddress address : IpUtils.parse(dcMeta.getZkServer().getAddress())){
			logger.info(remarkableMessage("[stopZkServer]{}"), address);
			stopServerListeningPort(address.getPort());
		}
		
		for(MetaServerMeta metaServerMeta : dcMeta.getMetaServers()){
			logger.info("[stopMetaServer]{}", metaServerMeta);
			stopServerListeningPort(metaServerMeta.getPort());
		}
		
		for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
			for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
				for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
					logger.info("[stopKeeperServer]{}", keeperMeta.getPort());
					stopServerListeningPort(keeperMeta.getPort());
				}
				for(RedisMeta redisMeta : shardMeta.getRedises()){
					logger.info("[stopRedisServer]{}", redisMeta.getPort());
					stopServerListeningPort(redisMeta.getPort());
				}
			}
		}
	}

	private void createDcs(){
		
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			dcs.put(dcMeta.getId(), new DcInfo(dcMeta));
		}
	}

	protected void startConsoleServer() throws Exception {
		
		logger.info(remarkableMessage("[startConsoleServer]{}"), consolePort);
		ConsoleStart consoleStart = new ConsoleStart(consolePort);
		consoleStart.initialize();
		consoleStart.start();
		add(consoleStart);
	}


	private void stopConsole() throws ExecuteException, IOException {
		stopServerListeningPort(consolePort);
	}


	protected boolean staticPort() {
		return false;
	}


	protected boolean stopIntegratedServers() {
		return true;
	}


	protected void startXpipe() throws Exception{
		
		logger.info(remarkableMessage("startXpipe"));
		
		startConsoleServer();
		
		for(String dc : getXpipeMeta().getDcs().keySet()){
			startDc(dc);
		}
	}
	

	protected void startDc(String dc) throws Exception {
		
		logger.info(remarkableMessage("[startDc]{}"), dc);

		DcMeta dcMeta = getXpipeMeta().getDcs().get(dc);
		DcInfo dcInfo = dcs.get(dc);
		if (dcMeta == null || dcInfo == null) {
			throw new IllegalStateException("dc not found:" + dc);
		}

		DefaultFoundationService.setDataCenter(dc);

		
		startZkServer(dcMeta.getZkServer());
		
		startMetaServers(dcMeta);


		LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcMeta);

		logger.info("[startDc]{}\n\n", dc);

		for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
			logger.info(remarkableMessage("[startCluster]{}"), clusterMeta.getId());
			for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
				logger.info(remarkableMessage("[startShard]{}"), shardMeta.getId());
				for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
					startKeeper(keeperMeta, leaderElectorManager);
				}
				for (RedisMeta redisMeta : shardMeta.getRedises()) {
					startRedis(redisMeta);
				}
			}
		}
	}

	protected void startMetaServers(DcMeta dcMeta) throws Exception {
		
		for(MetaServerMeta metaServerMeta : dcMeta.getMetaServers()){
			startMetaServer(metaServerMeta, dcMeta.getZkServer(), dcMeta);
		}
	}



	protected void stopAllRedisServer() throws IOException {
		executeScript("kill_redis.sh");
	}

	protected void startMetaServer(MetaServerMeta metaServerMeta, ZkServerMeta zkServerMeta, DcMeta dcMeta) throws Exception {
		
		logger.info(remarkableMessage("[startMetaServer]{}, {}"), metaServerMeta, zkServerMeta);
		
		DcInfo dcInfo = getDcInfos().get(dcMeta.getId());		
		
		TestMetaServer testMetaServer = new TestMetaServer(dcInfo.getIncreaseServerId(), metaServerMeta.getPort(), zkServerMeta.getAddress(), getXpipeMetaConfigFile());
		testMetaServer.initialize();
		testMetaServer.start();
		
		add(testMetaServer);
		
		dcInfo.setApplicationContext(testMetaServer.getContext());
	}

	public Set<String> getDcs() {
		return dcs.keySet();
	}

	protected Map<String, DcInfo> getDcInfos(){
		return this.dcs;
		
	}

	protected List<RedisKeeperServer> getRedisKeeperServers(String dc){
		
		List<RedisKeeperServer> result = new LinkedList<>();
		Map<String, RedisKeeperServer> redisKeeperServers = getRegistry().getComponents(RedisKeeperServer.class);
		for(RedisKeeperServer redisKeeperServer : redisKeeperServers.values()){
			String currentDc = ((ClusterMeta) redisKeeperServer.getCurrentKeeperMeta().parent().parent()).parent().getId();
			if(dc.equals(currentDc)){
				result.add(redisKeeperServer);
			}
			
		}
		return result;
	}

	protected void changeRedisMaster(RedisMeta redisMaster, RedisMeta toPromote) {
		
		for(DcMeta dcMeta : getXpipeMeta().getDcs().values()){
			for(ClusterMeta clusterMeta: dcMeta.getClusters().values()){
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					
					if(shardMeta.getRedises().remove(redisMaster)){
						for(RedisMeta redisMeta : shardMeta.getRedises()){
							if(redisMeta.getIp().equals(toPromote.getIp()) && redisMeta.getPort().equals(toPromote.getPort())){
								redisMeta.setMaster(null);
								return;
							}
						}
						throw new IllegalStateException("[can not find slave to promte]" + redisMaster + "," + toPromote); 
					}
				}
			}
		}
	}


	public int getConsolePort() {
		return consolePort;
	}

	@After
	public void afterAbstractIntegratedTest(){
		super.afterAbstractIntegratedTest();
	}

}
