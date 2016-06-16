package com.ctrip.xpipe.redis.integratedtest;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.junit.After;
import org.junit.Before;
import org.xml.sax.SAXException;

import com.ctrip.xpipe.redis.AbstractRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.DcMeta;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.entity.ShardMeta;
import com.ctrip.xpipe.redis.keeper.entity.XpipeMeta;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServerLocator;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaService;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServiceManager;
import com.ctrip.xpipe.redis.keeper.meta.MetaServiceManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.transform.DefaultSaxParser;
import com.ctrip.xpipe.redis.metaserver.StartMetaServer;
import com.ctrip.xpipe.utils.StringUtil;


/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class AbstractIntegratedTest extends AbstractRedisTest{

	private String integrated_test_config_file = "integrated-test.xml";
	
	private String redis_template = "conf/redis_template.conf";
	private StartMetaServer startMetaServer;
	
	private Map<String, DcInfo>  dcs = new ConcurrentHashMap<>();
	
	private int testMessageCount = 10000;
	
	private XpipeMeta xpipeMeta;
		
	@Before
	public void beforeAbstractIntegratedTest() throws Exception{
		
		dcs.put("jq", new DcInfo(9747, 2181));
		dcs.put("fq", new DcInfo(9748, 2182));
		loadXpipeMeta("integrated-test.xml");
		
		initRegistry();
		startRegistry();
		
		stopAllRedisServer();
		
		//stop meta server
		for(DcInfo dcInfo : dcs.values()){
			logger.info("[stopMetaServer]{}", dcInfo.getMetaServerPort());
			stopServerListeningPort(dcInfo.getMetaServerPort());
		}
		

		//stop keeper
		for(DcMeta dcMeta : xpipeMeta.getDcs().values()){
			for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
						logger.info("[stopKeeperServer]{}", keeperMeta.getPort());
						stopServerListeningPort(keeperMeta.getPort());
					}
				}
			}
		}
	}
	
	private void loadXpipeMeta(String string) throws SAXException, IOException {
		
		InputStream ins = getClass().getClassLoader().getResourceAsStream(integrated_test_config_file);
		xpipeMeta = DefaultSaxParser.parse(ins);
	}

	protected void startDc(String dc) throws Exception{
		

		logger.info(remarkableMessage("[startDc]{}"), dc);

		System.setProperty("idc", dc);
		DcMeta dcMeta = xpipeMeta.getDcs().get(dc);
		DcInfo dcInfo =  dcs.get(dc); 
		if(dcMeta == null || dcInfo == null){
			throw new IllegalStateException("dc not found:" + dc);
		}
		
		startMetaServer(dcInfo);
		
		MetaServiceManager metaServiceManager = createMetaServiceManager(dcInfo);
		
		
		for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
			logger.info(remarkableMessage("[startCluster]{}"), clusterMeta.getId());
			for(ShardMeta shardMeta : clusterMeta.getShards().values()){
				logger.info(remarkableMessage("[startShard]{}"), shardMeta.getId());
				for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
					startKeeper(dcInfo, keeperMeta, metaServiceManager);
				}
				for(RedisMeta redisMeta : shardMeta.getRedises()){
					startRedis(dcInfo, redisMeta);
				}
			}
		}
	}

	private MetaServiceManager createMetaServiceManager(DcInfo dcInfo) {
		
		DefaultMetaServerLocator metaServerLocator = new DefaultMetaServerLocator();
		metaServerLocator.setAddress(String.format("%s:%d", "localhost", dcInfo.getMetaServerPort()));
		
		DefaultMetaService metaService = new DefaultMetaService();
		metaService.setConfig(new DefaultKeeperConfig());
		metaService.setMetaServerLocator(metaServerLocator);
		
		DefaultMetaServiceManager metaServiceManager = new DefaultMetaServiceManager();
		metaServiceManager.setMetaService(metaService);
		return metaServiceManager;
	}

	private void startRedis(DcInfo dcInfo, RedisMeta redisMeta) throws ExecuteException, IOException {
		
		logger.info(remarkableMessage("[startRedis]{}, {}"), dcInfo, redisMeta);
		
		File testDir = new File(getTestFileDir());
		File redisDir = new File(testDir, "redisconfig");
		File dataDir = new File(redisDir, "data");
		File logDir = new File(redisDir, "logs");
		
		FileUtils.forceMkdir(dataDir);
		FileUtils.forceMkdir(logDir);
		
		File file = createRedisConfigFile(dcInfo, redisMeta, redisDir, dataDir);
		executeScript("start_redis.sh", file.getAbsolutePath(), new File(logDir, String.format("%d.log", redisMeta.getPort())).getAbsolutePath());
	}

	private File createRedisConfigFile(DcInfo dcInfo, RedisMeta redisMeta, File destDir, File dataDir) throws IOException {
		
		InputStream ins_template = getClass().getClassLoader().getResourceAsStream(redis_template);

		StringBuilder sb = new StringBuilder();
		for(String line : IOUtils.readLines(ins_template)){
			
			if(line.startsWith("#")){
				sb.append(line);
				continue;
			}
			
			String []confs = line.split("\\s+");
			if(confs.length < 2){
				sb.append(line);
				continue;
			}
			
			String confKey = confs[0];
			if(confKey.equalsIgnoreCase("port")){
				line = String.format("port %d", redisMeta.getPort());
			}
			if(confKey.equalsIgnoreCase("dir")){
				line = String.format("dir %s", dataDir.getAbsolutePath());
			}
			if(confKey.equalsIgnoreCase("meta-server-url")){
				line = String.format("meta-server-url http://localhost:%d/", dcInfo.getMetaServerPort());
			}
			if(confKey.equalsIgnoreCase("cluster-name")){
				line = String.format("cluster-name %s", redisMeta.parent().parent().getId());
			}
			if(confKey.equalsIgnoreCase("shard-name")){
				line = String.format("shard-name %s", redisMeta.parent().getId());
			}
			sb.append(line);
			sb.append("\r\n");
		}
		
		File dstFile = new File(destDir, redisMeta.getPort() + ".conf"); 
		IOUtils.write(sb, new FileOutputStream(dstFile));
		return dstFile;
	}

	private void startKeeper(DcInfo dcInfo, KeeperMeta keeperMeta, MetaServiceManager metaServiceManager) throws Exception {
		
		logger.info(remarkableMessage("[startKeeper]{}, {}"), dcInfo, keeperMeta);
		ReplicationStoreManager replicationStoreManager = new DefaultReplicationStoreManager(
				keeperMeta.parent().parent().getId(), keeperMeta.parent().getId(), 
				new File(getTestFileDir() + "/replication_store_" + keeperMeta.getPort()));
		
		RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(keeperMeta,replicationStoreManager, metaServiceManager);
		add(redisKeeperServer);
	}

	protected void stopAllRedisServer() throws ExecuteException, IOException {
		
		executeScript("kill_redis.sh");
	}

	protected void startMetaServer(DcInfo dcInfo) throws Exception {
		
		logger.info(remarkableMessage("[startMetaServer]{}"), dcInfo);
		startMetaServer = new StartMetaServer();
		startMetaServer.setZkPort(dcInfo.getZkPort());
		startMetaServer.setServerPort(dcInfo.getMetaServerPort());
		startMetaServer.start();
	}


	protected void stopServerListeningPort(int listenPort) throws ExecuteException, IOException {
		
		logger.info("[stopMetaServer]");
		executeScript("kill_server.sh", String.valueOf(listenPort));
	}

	protected void executeCommands(String ...args) throws ExecuteException, IOException{

		DefaultExecutor executor = new DefaultExecutor();
		executor.execute(CommandLine.parse(StringUtil.join(" ", args)));
	}
	
	protected void executeScript(String file, String ...args) throws ExecuteException, IOException{
		
		 URL url = getClass().getClassLoader().getResource(file);
		 if(url == null){
			 url = getClass().getClassLoader().getResource("scripts/" + file);
			 if(url == null){
				 throw new FileNotFoundException(file);
			 }
		 }
		DefaultExecutor executor = new DefaultExecutor();
		executor.execute(CommandLine.parse("sh -v " + url.getFile() + " " + StringUtil.join(" ", args)));
	}
	
	public Set<String> getDcs() {
		return dcs.keySet();
	}
	
	public XpipeMeta getXpipeMeta() {
		return xpipeMeta;
	}
	
	public int getTestMessageCount() {
		return testMessageCount;
	}
	
	
	public static class DcInfo{
		
		private int metaServerPort;
		private int zkPort;
		
		public DcInfo(int metaServerPort, int zkPort){
			this.metaServerPort = metaServerPort;
			this.zkPort = zkPort;
		}
		
		public int getMetaServerPort() {
			return metaServerPort;
		}
		
		public int getZkPort() {
			return zkPort;
		}
		
		@Override
		public String toString() {
			return String.format("zkPort:%d, metaServerPort:%d", zkPort, metaServerPort);
		}
	}

	@After
	public void afterAbstractIntegratedTest(){
		
	}
}
