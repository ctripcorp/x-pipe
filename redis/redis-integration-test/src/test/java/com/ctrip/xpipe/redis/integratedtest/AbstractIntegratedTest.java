package com.ctrip.xpipe.redis.integratedtest;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.cluster.DefaultLeaderElectorManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.*;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.GtidRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.NoneKeepersMonitorManager;
import com.ctrip.xpipe.redis.meta.server.job.XSlaveofJob;
import com.ctrip.xpipe.utils.DefaultLeakyBucket;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

/**
 * @author wenchao.meng
 *
 *         Jun 13, 2016
 */
public abstract class AbstractIntegratedTest extends AbstractRedisTest {

	private static final String logDir = "target/applogs";

	private String integrated_test_config_file = "integrated-test.xml";

	private String integratedPropertiesFile = "integration.properties";

	private Properties integratedProperties;

	private String clusterId = "cluster1", shardId = "shard1";

	private Long clusterDbId = 1L, shardDbId = 1L;

	private int defaultTestMessageCount = 5000;

	private Set<RedisMeta> allRedisStarted = new HashSet<>();

	protected KeeperResourceManager resourceManager = new DefaultKeeperResourceManager(
			new DefaultProxyEndpointManager(()->1000), new NaiveNextHopAlgorithm(), new DefaultLeakyBucket(100));

	@BeforeClass
	public static void beforereAbstractIntegratedTestClass(){
		List<File> result = new LinkedList<>();
		cleanLog(new File(logDir), result);
		Logger logger = LoggerFactory.getLogger(AbstractIntegratedTest.class);
		logger.info("[cleanLog]{}", result);
	}

	@Before
	public void beforeAbstractIntegratedTest() throws Exception {

		doBeforeIntegratedTest();
		
		integratedProperties = new Properties();
		integratedProperties.load(com.ctrip.xpipe.utils.FileUtils.getFileInputStream(integratedPropertiesFile));

		initRegistry();
		startRegistry();
	}

	protected void doBeforeIntegratedTest() throws Exception {
		
	}

	protected static void cleanLog(File log, List<File> cleanFiles) {

		if (log.isFile()) {
			cleanFiles.add(log);
			FileUtils.deleteQuietly(log);
			return;
		}
		if (log.isDirectory()) {
			for (File file : log.listFiles()) {
				cleanLog(file, cleanFiles);
			}
		}
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return integrated_test_config_file;
	}

	protected void startZkServer(ZkServerMeta zkServerMeta) {

		String[] addresses = zkServerMeta.getAddress().split("\\s*,\\s*");
		if (addresses.length != 1) {
			throw new IllegalStateException("zk server test should only be one there!" + zkServerMeta.getAddress());
		}

		String[] parts = addresses[0].split(":");
		if (parts.length != 2) {
			throw new IllegalStateException("zk address wrong:" + addresses[0]);
		}
		int zkPort = Integer.parseInt(parts[1]);
		startZk(zkPort);
	}

	protected RedisKeeperServer startKeeper(KeeperMeta keeperMeta,
			LeaderElectorManager leaderElectorManager) throws Exception {

		return startKeeper(keeperMeta, getKeeperConfig(), leaderElectorManager);
	}

	protected GtidRedisKeeperServer startGtidKeeper(KeeperMeta keeperMeta,
													LeaderElectorManager leaderElectorManager,
													RedisOpParser redisOpParser) throws Exception {
		return startGtidKeeper(keeperMeta, getKeeperConfig(), leaderElectorManager, redisOpParser);
	}

	protected KeeperConfig getKeeperConfig() {
		return new DefaultKeeperConfig();
	}

	protected GtidRedisKeeperServer startGtidKeeper(KeeperMeta keeperMeta, KeeperConfig keeperConfig,
												LeaderElectorManager leaderElectorManager,
												RedisOpParser redisOpParser) throws Exception {

		logger.info(remarkableMessage("[startGtidKeeper]{}, {}"), keeperMeta);
		File baseDir = new File(getTestFileDir() + "/replication_store_" + keeperMeta.getPort());

		GtidRedisKeeperServer gtidRedisKeeperServer = createGtidRedisKeeperServer(keeperMeta, baseDir, keeperConfig,
				leaderElectorManager, new NoneKeepersMonitorManager(), redisOpParser);
		add(gtidRedisKeeperServer);
		return gtidRedisKeeperServer;
	}

	protected RedisOpParser createRedisOpParser() {
		RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		RedisOpParser parser = new GeneralRedisOpParser(redisOpParserManager);
		new RedisOpMsetParser(redisOpParserManager);
		new RedisOpDelParser(redisOpParserManager);
		return parser;
	}

	protected RedisKeeperServer startKeeper(KeeperMeta keeperMeta, KeeperConfig keeperConfig,
			LeaderElectorManager leaderElectorManager) throws Exception {

		logger.info(remarkableMessage("[startKeeper]{}, {}"), keeperMeta);
		File baseDir = new File(getTestFileDir() + "/replication_store_" + keeperMeta.getPort());

		RedisKeeperServer redisKeeperServer = createRedisKeeperServer(keeperMeta, baseDir, keeperConfig, leaderElectorManager, new NoneKeepersMonitorManager());
		add(redisKeeperServer);
		return redisKeeperServer;
	}

	protected void stopKeeper(RedisKeeperServer redisKeeperServer) throws Exception {
		redisKeeperServer.stop();
		redisKeeperServer.dispose();
		remove(redisKeeperServer);
	}

	protected GtidRedisKeeperServer createGtidRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig,
																LeaderElectorManager leaderElectorManager,
																KeepersMonitorManager keeperMonitorManager,
																RedisOpParser redisOpParser) {

		return new GtidRedisKeeperServer(keeperMeta, keeperConfig, baseDir,
				leaderElectorManager, keeperMonitorManager, resourceManager, redisOpParser);
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig,
			 LeaderElectorManager leaderElectorManager, KeepersMonitorManager keeperMonitorManager) {

		return new DefaultRedisKeeperServer(keeperMeta, keeperConfig, baseDir,
				leaderElectorManager, keeperMonitorManager, resourceManager);
	}

	protected LeaderElectorManager createLeaderElectorManager(DcMeta dcMeta) throws Exception {

		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setZkAddress(dcMeta.getZkServer().getAddress());
		zkClient.initialize();
		zkClient.start();
		add(zkClient);

		DefaultLeaderElectorManager leaderElectorManager = new DefaultLeaderElectorManager(zkClient);
		leaderElectorManager.initialize();
		leaderElectorManager.start();
		add(leaderElectorManager);
		return leaderElectorManager;
	}

	protected void startRedis(RedisMeta redisMeta, RedisMeta redisMaster) throws IOException {

		stopServerListeningPort(redisMeta.getPort());

		logger.info(remarkableMessage("[startRedis]{}"), redisMeta);

		File testDir = new File(getTestFileDir());
		File redisDir = new File(testDir, "redisconfig");
		File dataDir = new File(redisDir, "data");
		File logDir = new File(redisDir, "logs");

		FileUtils.forceMkdir(dataDir);
		FileUtils.forceMkdir(logDir);

		File file = createRedisConfigFile(redisMeta, redisMaster, redisDir, dataDir);
		executeScript("start_redis.sh", file.getAbsolutePath(),
				new File(logDir, String.format("%d.log", redisMeta.getPort())).getAbsolutePath());

		allRedisStarted.add(redisMeta);
	}

	protected void startRedis(RedisMeta redisMeta) throws IOException {

		startRedis(redisMeta, null);
	}

	protected File createRedisConfigFile(RedisMeta redis, RedisMeta master, File destDir, File dataDir)
			throws IOException {

		String conf = getRedisConfig(redis, master, dataDir);

		File dstFile = new File(destDir, redis.getPort() + ".conf");
		try (FileOutputStream fous = new FileOutputStream(dstFile)) {
			IOUtils.write(conf, fous);
		}
		return dstFile;
	}

	protected String getRedisConfig(RedisMeta redis, RedisMeta master, File dataDir) throws IOException {

		StringBuilder sb = new StringBuilder();

		try (InputStream ins_template = getClass().getClassLoader().getResourceAsStream(getRedisTemplate())) {

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
					line = String.format("port %d", redis.getPort());
				}
				if (confKey.equalsIgnoreCase("dir")) {
					line = String.format("dir %s", dataDir.getAbsolutePath());
				}
				sb.append(line);
				sb.append("\r\n");
			}
		}

		if(master != null){
			sb.append(String.format("slaveof %s %d\r\n", master.getIp(), master.getPort()));
		}
		if(diskless()){
			sb.append("repl-diskless-sync yes\r\n");
			sb.append("repl-diskless-sync-delay " +integratedProperties.getProperty("redis.repl.diskless.delay", "1") + "\r\n");
		}else{
			sb.append("repl-diskless-sync no\r\n");
		}
		endPrepareRedisConfig(redis, sb);

		return sb.toString();
	}

	protected boolean diskless() {
		return Boolean.parseBoolean(integratedProperties.getProperty("redis.repl.diskless.sync", "false"));
	}

	protected void endPrepareRedisConfig(RedisMeta redisMeta, StringBuilder sb) {

	}

	protected void stopServerListeningPort(int listenPort) throws IOException {

		logger.info("[stopServerListeningPort]{}", listenPort);
		executeScript("kill_server.sh", String.valueOf(listenPort));
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getShardId() {
		return shardId;
	}

	public Long getClusterDbId() {
		return clusterDbId;
	}

	public Long getShardDbId() {
		return shardDbId;
	}

	protected void sendMesssageToMasterAndTest(RedisMeta redisMaster, List<RedisMeta> slaves){
		sendMesssageToMasterAndTest(defaultTestMessageCount, redisMaster, slaves);
	}

	protected void sendMesssageToMasterAndTest(int messageCount, RedisMeta redisMaster, List<RedisMeta> slaves){

		sendMessageToMaster(redisMaster, messageCount);
		sleep(2000);
		assertRedisEquals(redisMaster, slaves);
	}

	protected void sendMessageToMaster(){
		sendMessageToMaster(getRedisMaster(), defaultTestMessageCount);
	}

	protected void sendMessageToMaster(RedisMeta redisMaster){
		sendMessageToMaster(redisMaster, defaultTestMessageCount);
	}

	protected void sendMessageToMaster(RedisMeta redisMaster, int messageCount){
		sendRandomMessage(redisMaster, messageCount);
	}

	protected void sendMesssageToMasterAndTest(List<RedisMeta> slaves){
		
		sendMesssageToMasterAndTest(defaultTestMessageCount, getRedisMaster(), Lists.newArrayList(slaves));
	}

	protected void sendMessageToMasterAndTestSlaveRedis(int messageCount) {
		sendMesssageToMasterAndTest(messageCount, getRedisMaster(), getRedisSlaves());
	}
	
	protected void sendMessageToMasterAndTestSlaveRedis() {
		sendMesssageToMasterAndTest(defaultTestMessageCount, getRedisMaster(), getRedisSlaves());
	}

	protected abstract List<RedisMeta> getRedisSlaves();

	protected List<RedisMeta> getAllRedisSlaves() {

		List<RedisMeta> result = new LinkedList<>();
		for (DcMeta dcMeta : getDcMetas()) {
			List<RedisMeta> slaves = getRedisSlaves(dcMeta.getId());
			Assert.assertTrue(slaves.size() >= 1);
			result.addAll(slaves);
		}
		Assert.assertTrue(result.size() >= 1);
		return result;
	}

	public RedisKeeperServer getRedisKeeperServerActive(String dc) {

		Map<String, RedisKeeperServer> redisKeeperServers = getRegistry().getComponents(RedisKeeperServer.class);

		for (RedisKeeperServer server : redisKeeperServers.values()) {
			String currentDc = ((ClusterMeta) server.getCurrentKeeperMeta().parent().parent()).parent().getId();
			if (dc.equals(currentDc) && server.getRedisKeeperServerState().keeperState().isActive()) {
				return server;
			}
		}
		return null;
	}

	public RedisKeeperServer getRedisKeeperServer(KeeperMeta keeperMeta) {

		Map<String, RedisKeeperServer> redisKeeperServers = getRegistry().getComponents(RedisKeeperServer.class);

		for (RedisKeeperServer server : redisKeeperServers.values()) {
			KeeperMeta currentKeeperMeta = server.getCurrentKeeperMeta();
			if (MetaUtils.same(currentKeeperMeta, keeperMeta)) {
				return server;
			}
		}
		return null;
	}

	protected KeeperMeta getKeeperActive(String dc) {

		for (KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())) {
			if (keeperMeta.isActive()) {
				return keeperMeta;
			}
		}
		return null;
	}

	protected List<KeeperMeta> getKeepersBackup(String dc) {

		List<KeeperMeta> result = new LinkedList<>();
		for (KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())) {
			if (!keeperMeta.isActive()) {
				result.add(keeperMeta);
			}
		}
		return result;
	}

	protected void assertRedisEquals() {
		assertRedisEquals(getRedisMaster(), getRedisSlaves());
	}

	@After
	public void afterAbstractIntegratedTest() {

		for (RedisMeta redisMeta : allRedisStarted) {
			try {
				logger.info("[afterAbstractIntegratedTest][stop redis]{}", redisMeta.desc());
				stopServerListeningPort(redisMeta.getPort());
			} catch (IOException e) {
				logger.error("[afterAbstractIntegratedTest][error stop redis]" + redisMeta, e);
			}
		}
	}

	protected void xslaveof(String masterIp, Integer masterPort, RedisMeta ... slaves) throws Exception {
		
		new XSlaveofJob(Lists.newArrayList(slaves), masterIp, masterPort, getXpipeNettyClientKeyedObjectPool(), scheduled, executors).execute().sync();
	}

	protected void xslaveof(String masterIp, Integer masterPort, List<RedisMeta> slaves) throws Exception {
		
		new XSlaveofJob(slaves, masterIp, masterPort, getXpipeNettyClientKeyedObjectPool(), scheduled, executors).execute().sync();
	}


	@Override
	protected boolean deleteTestDirAfterTest() {
		return false;
	}

	protected String getRedisTemplate() {
		return "conf/redis_raw.conf";
	}

	protected String getCasePath(String caseName) {
		String path = "cases/" + caseName;
		URL url = getClass().getResource(path);
		if (url == null) {
		    url = getClass().getClassLoader().getResource(path);
		}
		if (url == null) {
		    return null;
		} else {
			return url.getPath();
		}
	}

	protected void prepareCaseIfExist() throws IOException {
		String casePath = getCasePath(getClass().getSimpleName() + "-" + getTestName());
		if (casePath != null) {
			executeCommands("cp", "-rf", casePath + "/.", getTestFileDir());
		}
	}

	protected void waitSlaveOnline(String redisIp, int redisPort) throws Exception {
		waitConditionUntilTimeOut(() -> {
			try {
				return infoRedis(redisIp, redisPort, InfoCommand.INFO_TYPE.REPLICATION, "master_link_status")
						.equalsIgnoreCase("up");
			} catch (Exception e) {
				return false;
			}
		}, 5000, 1000);
	}

	protected String infoRedis(String redisIp, int redisPort, InfoCommand.INFO_TYPE infoType, String key) throws Exception {
		SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(redisIp, redisPort));
		InfoCommand cmd = new InfoCommand(keyPool, infoType, scheduled);
		cmd.logRequest(false);
		cmd.logResponse(false);
		String info = cmd.execute().get();
		InfoResultExtractor extractor = new InfoResultExtractor(info);
		return extractor.extract(key);
	}


}
