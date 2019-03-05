package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.spring.KeeperContextConfig;
import org.junit.Before;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         Jun 12, 2016
 */
public class AbstractRedisKeeperContextTest extends AbstractRedisKeeperTest {

	protected KeeperConfig  keeperConfig;

	private ProxyResourceManager proxyResourceManager;
	
	private String keeperConfigFile = "keeper6666.xml";

	private int keeperServerPortMin = 7777, keeperServerPortMax = 7877;

	@Before
	public void beforeAbstractRedisKeeperTest() throws Exception {
		
		doIdcInit();
		
		keeperConfig = getRegistry().getComponent(KeeperConfig.class);
		proxyResourceManager = getRegistry().getComponent(ProxyResourceManager.class);
		
	}
	
	@Override
	protected ConfigurableApplicationContext createSpringContext() {
		return new AnnotationConfigApplicationContext(KeeperContextConfig.class);
	}

	protected void doIdcInit() {
	}

	protected KeeperMeta createKeeperMeta() throws SAXException, IOException {

		return createKeeperMeta(randomPort(keeperServerPortMin, keeperServerPortMax));
	}


	protected KeeperMeta createKeeperMeta(int port) throws SAXException, IOException {

		XpipeMeta xpipe = loadXpipeMeta(getXpipeMetaConfigFile());
		for(DcMeta dcMeta : xpipe.getDcs().values()){
			for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
						keeperMeta.setPort(port);
						keeperMeta.setActive(true);
						keeperMeta.setId(randomString(40));
						return keeperMeta;
					}
				}
			}
		}
		return null;
	}

	protected String getKeeperConfigFile() {
		return keeperConfigFile;
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperConfig keeperConfig) throws Exception {
		
		return createRedisKeeperServer(createKeeperMeta(), keeperConfig, getReplicationStoreManagerBaseDir());
	}
	
	protected RedisKeeperServer createRedisKeeperServer() throws Exception {

		return createRedisKeeperServer(createKeeperMeta());
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeper) throws Exception {
		return createRedisKeeperServer(keeper, getReplicationStoreManagerBaseDir());
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeper, File baseDir) throws Exception {

		return createRedisKeeperServer(keeper, getKeeperConfig(), baseDir);

	}

	protected KeeperConfig getKeeperConfig() {
		return keeperConfig;
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeper, KeeperConfig keeperConfig, File baseDir) throws Exception {

		return createRedisKeeperServer(keeper, keeperConfig, baseDir, getRegistry().getComponent(LeaderElectorManager.class));
	}

	protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeper, KeeperConfig keeperConfig,
			File baseDir, LeaderElectorManager leaderElectorManager) {
		return new DefaultRedisKeeperServer(keeper, keeperConfig, baseDir, leaderElectorManager,
				createkeepersMonitorManager(), proxyResourceManager);
	}

	protected RedisMeta createRedisMeta() {
		
		return createRedisMeta("localhost", randomPort());
	}

	protected RedisMeta createRedisMeta(String host, int port) {
		
		RedisMeta redisMeta = new RedisMeta();
		redisMeta.setIp(host);
		redisMeta.setPort(port);
		return redisMeta;
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return null;
	}
}
