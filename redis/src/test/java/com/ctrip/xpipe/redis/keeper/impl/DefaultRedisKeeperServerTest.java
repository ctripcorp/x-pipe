package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.ctrip.xpipe.redis.AbstractRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.entity.ClusterMeta;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.ShardMeta;
import com.ctrip.xpipe.redis.keeper.entity.XpipeMeta;
import com.ctrip.xpipe.redis.keeper.meta.MetaServiceManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.transform.DefaultSaxParser;
import com.ctrip.xpipe.redis.spring.KeeperContextConfig;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午5:42:29
 */
public class DefaultRedisKeeperServerTest extends AbstractRedisTest{

	protected AnnotationConfigApplicationContext springCtx;
	
	protected MetaServiceManager metaServiceManager;
	
	@Before
	public void beforeDefaultRedisKeeperServerTest(){
		
		springCtx = new AnnotationConfigApplicationContext(KeeperContextConfig.class);
		metaServiceManager = springCtx.getBean(MetaServiceManager.class);
	}

	@Test
	public void startKeeper6666() throws Exception {
		
		startKeeper("keeper6666.xml");
	}

	@Test
	public void startKeeper7777() throws Exception {

		startKeeper("keeper7777.xml");
	}


	private void startKeeper(String keeperConfigFile) throws Exception {
		
		XpipeMeta xpipe = DefaultSaxParser.parse(getClass().getClassLoader().getResourceAsStream(keeperConfigFile));
		ClusterMeta cluster = xpipe.getClusters().get(0);

		DefaultKeeperConfig config = new DefaultKeeperConfig();
		setupZkNodes(cluster, config);
		startKeepers(cluster);
	}

   private void startKeepers(final ClusterMeta cluster) throws Exception {

		for (final ShardMeta shard : cluster.getShards()) {
			
			int index = 0;
			for (final KeeperMeta keeper : shard.getKeepers()) {
				
				File storeDir = new File(getTestFileDir() + "/" + index);
				logger.info("[startKeepers]{},{},{}", cluster.getId(), shard.getId(), storeDir);
				ReplicationStoreManager  replicationStoreManager = new DefaultReplicationStoreManager(cluster.getId(), 
						shard.getId(), storeDir);
				RedisKeeperServer redisKeeperServer = new DefaultRedisKeeperServer(cluster.getId(), shard.getId(), keeper, replicationStoreManager, metaServiceManager);
				redisKeeperServer.initialize();
				redisKeeperServer.start();
				index++;
			}
		}
	}

	private void setupZkNodes(ClusterMeta cluster, KeeperConfig config) throws Exception {
		CuratorFramework client = initializeZK(config);
		for (ShardMeta shard : cluster.getShards()) {
			String path = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), cluster.getId(), shard.getId());
			client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());
		}
	}

	private CuratorFramework initializeZK(KeeperConfig config) throws InterruptedException {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(config.getZkConnectionTimeoutMillis());
		builder.connectString(config.getZkConnectionString());
		builder.maxCloseWaitMs(config.getZkCloseWaitMillis());
		builder.namespace(config.getZkNamespace());
		builder.retryPolicy(new RetryNTimes(3, config.getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(config.getZkSessionTimeoutMillis());

		CuratorFramework client = builder.build();
		client.start();
		client.blockUntilConnected();

		return client;
	}

	@After
	public void afterOneBoxTest() throws IOException{
		
		System.out.println("Press any key to exit");
		System.in.read();
	}
}
