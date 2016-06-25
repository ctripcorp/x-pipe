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
import org.apache.curator.framework.CuratorFramework;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.junit.After;
import org.junit.Before;
import org.springframework.context.ApplicationContext;

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
import com.ctrip.xpipe.redis.core.impl.AbstractCoreConfig;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServerLocator;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaService;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServiceManager;
import com.ctrip.xpipe.redis.keeper.meta.MetaServiceManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.zk.ZkConfig;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;

/**
 * @author wenchao.meng
 *
 *         Jun 13, 2016
 */
public abstract class AbstractIntegratedTest extends AbstractRedisTest {

	private String integrated_test_config_file = "integrated-test.xml";

	private String redis_template = "conf/redis_template.conf";
	private MetaServerPrepareResourcesAndStart startMetaServer;
	
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

		stopConsole();
		
		//stop all servers
		stopAllRedisServer();
				
		// stop keeper
		for (DcMeta dcMeta : getXpipeMeta().getDcs().values()) {

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
		new ConsoleStart(consolePort).start();
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

		MetaServiceManager metaServiceManager = createMetaServiceManager(dcMeta.getMetaServers());
		
		LeaderElectorManager leaderElectorManager = createLeaderElectorManager(dcMeta);

		logger.info("[startDc]{}\n\n", dc);

		for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
			logger.info(remarkableMessage("[startCluster]{}"), clusterMeta.getId());
			for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
				logger.info(remarkableMessage("[startShard]{}"), shardMeta.getId());
				for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
					startKeeper(keeperMeta, metaServiceManager, leaderElectorManager);
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
		
		createLeaderElectManager(dcMeta.getZkServer().getAddress());
	}

	/**
	 * @param address
	 */
	private void createLeaderElectManager(String address) {
		
		ZkConfig config = new DefaultZkConfig();
		CuratorFramework client = config.create(address);
		
		DefaultLeaderElectorManager leaderElectorManager = new DefaultLeaderElectorManager(client);
		
		DefaultZkClient zkClient = new DefaultZkClient();
		AbstractCoreConfig coreConfig = new AbstractCoreConfig();
		coreConfig.setZkConnectionString(dcMeta.getZkServer().getAddress());
		zkClient.setConfig(coreConfig);
		zkClient.initialize();
		zkClient.start();
		
		
		leaderElectorManager.setZkClient(zkClient);
		return leaderElectorManager;
		
	}

	protected MetaServiceManager createMetaServiceManager(List<MetaServerMeta> metaServerMetas) {

		DefaultMetaServerLocator metaServerLocator = new DefaultMetaServerLocator();
		metaServerLocator.setAddress(String.format("%s:%d", "localhost", metaServerMetas.get(0).getPort()));

		DefaultMetaService metaService = new DefaultMetaService();
		metaService.setConfig(new DefaultKeeperConfig());
		metaService.setMetaServerLocator(metaServerLocator);

		DefaultMetaServiceManager metaServiceManager = new DefaultMetaServiceManager();
		metaServiceManager.setMetaService(metaService);
		return metaServiceManager;
	}

	protected void startRedis(DcMeta dcMeta, RedisMeta redisMeta) throws ExecuteException, IOException {
		
		logger.info(remarkableMessage("[startRedis]{}, {}"), dcMeta, redisMeta);
		
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

		InputStream ins_template = getClass().getClassLoader().getResourceAsStream(redis_template);
		
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
		IOUtils.write(sb, new FileOutputStream(dstFile));
		return dstFile;
	}

	protected void startKeeper(KeeperMeta keeperMeta, MetaServiceManager metaServiceManager, LeaderElectorManager leaderElectorManager) throws Exception {

		logger.info(remarkableMessage("[startKeeper]{}, {}"), keeperMeta);
		ReplicationStoreManager replicationStoreManager = new DefaultReplicationStoreManager(
				keeperMeta.parent().parent().getId(), keeperMeta.parent().getId(), 
				new File(getTestFileDir() + "/replication_store_" + keeperMeta.getPort()));

		RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeperMeta, replicationStoreManager, metaServiceManager, leaderElectorManager);
		add(redisKeeperServer);
	}

	protected void stopAllRedisServer() throws ExecuteException, IOException {

		executeScript("kill_redis.sh");
	}

	protected void startMetaServer(MetaServerMeta metaServerMeta, ZkServerMeta zkServerMeta, DcMeta dcMeta) throws Exception {
		
		logger.info(remarkableMessage("[startMetaServer]{}"), metaServerMeta);
		
//		startZk(dcInfo.getZkPort());
		
		startMetaServer = new MetaServerPrepareResourcesAndStart(zkServerMeta.getAddress(), metaServerMeta.getPort());
		ApplicationContext applicationContext = startMetaServer.start(dcMeta);
		dcs.get(dcMeta.getId()).setApplicationContext(applicationContext);
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
								redisMeta.setMaster(true);
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
