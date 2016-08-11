package com.ctrip.xpipe.redis.integratedtest;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.junit.After;
import org.junit.Before;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.DefaultLeaderElectorManager;
import com.ctrip.xpipe.foundation.FakeFoundationService;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServerLocator;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaService;
import com.ctrip.xpipe.redis.keeper.meta.MetaService;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 *         Jun 13, 2016
 */
public abstract class AbstractIntegratedTest extends AbstractRedisTest {

	private String integrated_test_config_file = "integrated-test.xml";

	private String redis_template = "conf/redis_template.conf";
	
	private Map<String, DcInfo>  dcs = new ConcurrentHashMap<>();
	
	private int consolePort = 8080;
	
	private int testMessageCount = 10000;

	@Before
	public void beforeAbstractIntegratedTest() throws Exception {
		
		
		createDcs();

		initRegistry();
		startRegistry();
		
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

		FakeFoundationService.setDataCenter(dc);

		
		startZkServer(dcMeta.getZkServer());
		
		startMetaServers(dcMeta);

		MetaService metaService = createMetaService(dcMeta.getMetaServers());
		
		LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcMeta);

		logger.info("[startDc]{}\n\n", dc);

		for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
			logger.info(remarkableMessage("[startCluster]{}"), clusterMeta.getId());
			for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
				logger.info(remarkableMessage("[startShard]{}"), shardMeta.getId());
				for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
					startKeeper(keeperMeta, metaService, leaderElectorManager);
				}
				for (RedisMeta redisMeta : shardMeta.getRedises()) {
					startRedis(dcMeta, redisMeta);
				}
			}
		}
	}

	protected void startMetaServers(DcMeta dcMeta) throws Exception {
		
		for(MetaServerMeta metaServerMeta : dcMeta.getMetaServers()){
			startMetaServer(metaServerMeta, dcMeta.getZkServer(), dcMeta);
		}
	}

	protected void startZkServer(ZkServerMeta zkServerMeta) {
		
		String []addresses = zkServerMeta.getAddress().split("\\s*,\\s*");
		if(addresses.length != 1){
			throw new IllegalStateException("zk server test should only be one there!" + zkServerMeta.getAddress());
		}
		
		String []parts = addresses[0].split(":");
		if(parts.length != 2){
			throw new IllegalStateException("zk address wrong:" + addresses[0]);
		}
		int zkPort = Integer.parseInt(parts[1]);
		startZk(zkPort);
	}

	protected LeaderElectorManager createLeaderElectorManager(DcMeta dcMeta) throws Exception {
		
		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setZkAddress(dcMeta.getZkServer().getAddress());

	
		DefaultLeaderElectorManager leaderElectorManager = new DefaultLeaderElectorManager(zkClient);
		leaderElectorManager.initialize();
		leaderElectorManager.start();
		
		return leaderElectorManager;
	}


	protected MetaService createMetaService(List<MetaServerMeta> metaServerMetas) {

		DefaultMetaServerLocator metaServerLocator = new DefaultMetaServerLocator();
		metaServerLocator.setAddress(String.format("http://%s:%d", "localhost", metaServerMetas.get(0).getPort()));

		DefaultMetaService metaService = new DefaultMetaService();
		metaService.setConfig(new DefaultKeeperConfig());
		metaService.setMetaServerLocator(metaServerLocator);

		return metaService;
	}

	protected void startRedis(DcMeta dcMeta, RedisMeta redisMeta) throws ExecuteException, IOException {
		
		stopServerListeningPort(redisMeta.getPort());
		
		logger.info(remarkableMessage("[startRedis]{}"), redisMeta);
		
		File testDir = new File(getTestFileDir());
		File redisDir = new File(testDir, "redisconfig");
		File dataDir = new File(redisDir, "data");
		File logDir = new File(redisDir, "logs");

		FileUtils.forceMkdir(dataDir);
		FileUtils.forceMkdir(logDir);

		File file = createRedisConfigFile(dcMeta, redisMeta, redisDir, dataDir);
		executeScript("start_redis.sh", file.getAbsolutePath(), new File(logDir, String.format("%d.log", redisMeta.getPort())).getAbsolutePath());
	}

	private File createRedisConfigFile(DcMeta dcMeta, RedisMeta redisMeta, File destDir, File dataDir) throws IOException {

		try(InputStream ins_template = getClass().getClassLoader().getResourceAsStream(redis_template)){
			int metaServerPort = dcMeta.getMetaServers().get(0).getPort();
	
			StringBuilder sb = new StringBuilder();
			for (String line : IOUtils.readLines(ins_template)) {
	
				if (line.startsWith("#")) {
					sb.append(line);
					continue;
				}
	
				String[] confs = line.split("\\s+");
				if (confs.length < 2) {
					sb.append(line);
					continue;
				}
	
				String confKey = confs[0];
				if (confKey.equalsIgnoreCase("port")) {
					line = String.format("port %d", redisMeta.getPort());
				}
				if (confKey.equalsIgnoreCase("dir")) {
					line = String.format("dir %s", dataDir.getAbsolutePath());
				}
				if (confKey.equalsIgnoreCase("meta-server-url")) {
					line = String.format("meta-server-url http://localhost:%d/", metaServerPort);
				}
				if (confKey.equalsIgnoreCase("cluster-name")) {
					line = String.format("cluster-name %s", redisMeta.parent().parent().getId());
				}
				if (confKey.equalsIgnoreCase("shard-name")) {
					line = String.format("shard-name %s", redisMeta.parent().getId());
				}
				sb.append(line);
				sb.append("\r\n");
			}
	
			
			
			File dstFile = new File(destDir, redisMeta.getPort() + ".conf");
			try(FileOutputStream fous = new FileOutputStream(dstFile)){
				IOUtils.write(sb, fous);
			}
			return dstFile;
		}
	}

	protected void startKeeper(KeeperMeta keeperMeta, MetaService metaService, LeaderElectorManager leaderElectorManager) throws Exception {

		logger.info(remarkableMessage("[startKeeper]{}, {}"), keeperMeta);
		File baseDir = new File(getTestFileDir() + "/replication_store_" + keeperMeta.getPort());

		RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeperMeta, baseDir, metaService, leaderElectorManager);
		add(redisKeeperServer);
	}

	protected void stopAllRedisServer() throws ExecuteException, IOException {

		executeScript("kill_redis.sh");
	}

	protected void startMetaServer(MetaServerMeta metaServerMeta, ZkServerMeta zkServerMeta, DcMeta dcMeta) throws Exception {
		
		logger.info(remarkableMessage("[startMetaServer]{}, {}"), metaServerMeta, zkServerMeta);
				
		MetaServerPrepareResourcesAndStart startMetaServer = new MetaServerPrepareResourcesAndStart(integrated_test_config_file, zkServerMeta.getAddress(), metaServerMeta.getPort(), dcMeta);
		startMetaServer.initialize();
		startMetaServer.start();
		
		add(startMetaServer);
		
		dcs.get(dcMeta.getId()).setApplicationContext(startMetaServer.getApplicationContext());
	}

	protected void stopServerListeningPort(int listenPort) throws ExecuteException, IOException {

		logger.info("[stopServerListeningPort]{}", listenPort);
		executeScript("kill_server.sh", String.valueOf(listenPort));
	}

	public Set<String> getDcs() {
		return dcs.keySet();
	}

	public int getTestMessageCount() {
		return testMessageCount;
	}
	
	
	
	protected Map<String, DcInfo> getDcInfos(){
		return this.dcs;
		
	}

	protected List<RedisKeeperServer> getRedisKeeperServers(String dc){
		
		List<RedisKeeperServer> result = new LinkedList<>();
		Map<String, RedisKeeperServer> redisKeeperServers = getRegistry().getComponents(RedisKeeperServer.class);
		for(RedisKeeperServer redisKeeperServer : redisKeeperServers.values()){
			String currentDc = redisKeeperServer.getCurrentKeeperMeta().parent().parent().parent().getId();
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

	public RedisKeeperServer getRedisKeeperServerActive(String dc){
		
		Map<String, RedisKeeperServer> redisKeeperServers = getRegistry().getComponents(RedisKeeperServer.class);
		
		for(RedisKeeperServer server : redisKeeperServers.values()){
			String currentDc =server.getCurrentKeeperMeta().parent().parent().parent().getId(); 
			if(dc.equals(currentDc)  && server.getRedisKeeperServerState().isActive()){
				return server;
			}
		}
		return null;
	}

	public int getConsolePort() {
		return consolePort;
	}
	
	@Override
	protected String getXpipeMetaConfigFile() {
		return integrated_test_config_file;
	}

	protected abstract List<RedisMeta> getRedisSlaves();
	
	@After
	public void afterAbstractIntegratedTest(){
		
	}
}
