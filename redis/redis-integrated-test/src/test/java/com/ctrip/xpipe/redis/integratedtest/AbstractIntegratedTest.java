package com.ctrip.xpipe.redis.integratedtest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.DefaultLeaderElectorManager;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServerLocator;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaService;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 *         Jun 13, 2016
 */
public abstract class AbstractIntegratedTest extends AbstractRedisTest {
	
	private String integrated_test_config_file = "integrated-test.xml";
	
	private String clusterId = "cluster1", shardId = "shard1";
	
	private int defaultTestMessageCount = 10000;

	@Before
	public void beforeAbstractIntegratedTest() throws Exception{
		
		initRegistry();
		startRegistry();
	}

	
	@Override
	protected String getXpipeMetaConfigFile() {
		return integrated_test_config_file;
	}


	public String getIntegrated_test_config_file() {
		return integrated_test_config_file;
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
	
	protected void startKeeper(KeeperMeta keeperMeta, MetaServerKeeperService metaService, LeaderElectorManager leaderElectorManager) throws Exception {
		
		startKeeper(keeperMeta, getKeeperConfig(), metaService, leaderElectorManager);
	}

	protected KeeperConfig getKeeperConfig() {
		return new DefaultKeeperConfig();
	}

	protected void startKeeper(KeeperMeta keeperMeta, KeeperConfig keeperConfig, MetaServerKeeperService metaService, LeaderElectorManager leaderElectorManager) throws Exception {

		logger.info(remarkableMessage("[startKeeper]{}, {}"), keeperMeta);
		File baseDir = new File(getTestFileDir() + "/replication_store_" + keeperMeta.getPort());

		RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeperMeta, keeperConfig, baseDir, metaService, leaderElectorManager);
		add(redisKeeperServer);
	}

	protected LeaderElectorManager createLeaderElectorManager(DcMeta dcMeta) throws Exception {
		
		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setZkAddress(dcMeta.getZkServer().getAddress());

	
		DefaultLeaderElectorManager leaderElectorManager = new DefaultLeaderElectorManager(zkClient);
		leaderElectorManager.initialize();
		leaderElectorManager.start();
		
		return leaderElectorManager;
	}


	protected MetaServerKeeperService createMetaService(List<MetaServerMeta> metaServerMetas) {

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

	protected File createRedisConfigFile(DcMeta dcMeta, RedisMeta redisMeta, File destDir, File dataDir) throws IOException {

		try(InputStream ins_template = getClass().getClassLoader().getResourceAsStream(getRedisTemplate())){
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
			
			endPrepareRedisConfig(redisMeta, sb);
			
			File dstFile = new File(destDir, redisMeta.getPort() + ".conf");
			try(FileOutputStream fous = new FileOutputStream(dstFile)){
				IOUtils.write(sb, fous);
			}
			return dstFile;
		}
	}

	protected void endPrepareRedisConfig(RedisMeta redisMeta, StringBuilder sb) {
		
	}

	protected void stopServerListeningPort(int listenPort) throws ExecuteException, IOException {

		logger.info("[stopServerListeningPort]{}", listenPort);
		executeScript("kill_server.sh", String.valueOf(listenPort));
	}
	
	public String getClusterId() {
		return clusterId;
	}

	public String getShardId() {
		return shardId;
	}

	protected abstract String getRedisTemplate();
	
	protected void sendMesssageToMasterAndTest(int messageCount, RedisMeta ... slaves){

		sendRandomMessage(getRedisMaster(), messageCount);
		sleep(6000);
		assertRedisEquals(getRedisMaster(), slaves);
	}

	protected void sendMesssageToMasterAndTest(RedisMeta ... slaves){
		sendMesssageToMasterAndTest(defaultTestMessageCount, slaves);
	}

	
	protected void sendMessageToMasterAndTestSlaveRedis() {
		sendMesssageToMasterAndTest(getRedisSlaves().toArray(new RedisMeta[0]));
	}

	protected abstract List<RedisMeta> getRedisSlaves();

	protected List<RedisMeta> getAllRedisSlaves() {
		
		List<RedisMeta> result = new LinkedList<>();
		for(DcMeta dcMeta : getDcMetas()){
			List<RedisMeta> slaves = getRedisSlaves(dcMeta.getId());
			Assert.assertTrue(slaves.size() >= 1);
			result.addAll(slaves);
		}
		Assert.assertTrue(result.size() >= 1);
		return result;
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

	public RedisKeeperServer getRedisKeeperServer(KeeperMeta keeperMeta){
		
		Map<String, RedisKeeperServer> redisKeeperServers = getRegistry().getComponents(RedisKeeperServer.class);
		
		for(RedisKeeperServer server : redisKeeperServers.values()){
			KeeperMeta currentKeeperMeta = server.getCurrentKeeperMeta();
			if(MetaUtils.same(currentKeeperMeta, keeperMeta)){
				return server;
			}
		}
		return null;
	}

	protected KeeperMeta getKeeperActive(String dc){
		
		for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
			if(keeperMeta.isActive()){
				return keeperMeta;
			}
		}
		return null;
	}

	protected List<KeeperMeta> getKeepersBackup(String dc){
		
		List<KeeperMeta> result = new LinkedList<>();
		for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())){
			if(!keeperMeta.isActive()){
				result.add(keeperMeta);
			}
		}
		return result;
	}

}
