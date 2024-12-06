package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.ReplDelayConfigCache;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.spring.KeeperContextConfig;
import org.junit.Before;
import org.mockito.Mockito;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *
 *         Jun 12, 2016
 */
public class AbstractRedisKeeperContextTest extends AbstractRedisKeeperTest {

	protected KeeperConfig  keeperConfig;

	private KeeperResourceManager resourceManager;
	
	private String keeperConfigFile = "keeper6666.xml";

	private int keeperServerPortMin = 7777, keeperServerPortMax = 7877;

	@Before
	public void beforeAbstractRedisKeeperTest() throws Exception {
		
		doIdcInit();
		
		keeperConfig = getRegistry().getComponent(KeeperConfig.class);
		resourceManager = getRegistry().getComponent(KeeperResourceManager.class);
		
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

	protected  KeeperMeta createKeeperMeta(int port) throws SAXException, IOException {
		return createKeeperMeta(port, randomString(40));
	}

	protected KeeperMeta createKeeperMeta(int port, String runId) throws SAXException, IOException {

		XpipeMeta xpipe = loadXpipeMeta(getXpipeMetaConfigFile());
		for(DcMeta dcMeta : xpipe.getDcs().values()){
			for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
			    clusterMeta.setDbId(getClusterId().id());
				for(ShardMeta shardMeta : clusterMeta.getShards().values()){
					shardMeta.setDbId(getShardId().id());
					for(KeeperMeta keeperMeta : shardMeta.getKeepers()){
						keeperMeta.setPort(port);
						keeperMeta.setActive(true);
						keeperMeta.setId(runId);
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

		KeeperMeta keeperMeta = createKeeperMeta();
		ReplId replId = getReplId();
		return createRedisKeeperServer(replId.id(), keeperMeta, keeperConfig, getReplicationStoreManagerBaseDir(keeperMeta));
	}
	
	protected RedisKeeperServer createRedisKeeperServer() throws Exception {

		return createRedisKeeperServer(getReplId().id(), createKeeperMeta());
	}

	protected RedisKeeperServer createRedisKeeperServer(Long replId, KeeperMeta keeper) throws Exception {
		return createRedisKeeperServer(replId, keeper, getReplicationStoreManagerBaseDir(keeper));
	}

	protected RedisKeeperServer createRedisKeeperServer(Long replId, KeeperMeta keeper, File baseDir) throws Exception {

		return createRedisKeeperServer(replId, keeper, getKeeperConfig(), baseDir);

	}

	protected KeeperConfig getKeeperConfig() {
		return keeperConfig;
	}

	protected RedisKeeperServer createRedisKeeperServer(Long replId, KeeperMeta keeper, KeeperConfig keeperConfig, File baseDir) throws Exception {

		return createRedisKeeperServer(replId, keeper, keeperConfig, baseDir, getRegistry().getComponent(LeaderElectorManager.class));
	}

	protected RedisKeeperServer createRedisKeeperServer(Long replId, KeeperMeta keeper, KeeperConfig keeperConfig,
			File baseDir, LeaderElectorManager leaderElectorManager) {
		return new DefaultRedisKeeperServer(replId, keeper, keeperConfig, baseDir, leaderElectorManager,
				createkeepersMonitorManager(), getResourceManager(), Mockito.mock(SyncRateManager.class), createRedisOpParser(), new ReplDelayConfigCache());
	}

	protected RedisOpParser createRedisOpParser() {
		RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
		RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
		RedisOpParser parser = new GeneralRedisOpParser(redisOpParserManager);
		return parser;
	}

	protected KeeperResourceManager getResourceManager() {
		return resourceManager;
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

	protected void waitRedisKeeperServerConnected(RedisKeeperServer redisKeeperServer) throws TimeoutException {
		waitConditionUntilTimeOut(()->{return redisKeeperServer.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED;});
	}


	@Override
	protected String getXpipeMetaConfigFile() {
		return null;
	}
}
