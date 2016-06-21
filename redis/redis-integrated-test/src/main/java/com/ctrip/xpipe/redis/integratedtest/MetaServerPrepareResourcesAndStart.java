package com.ctrip.xpipe.redis.integratedtest;

import java.io.File;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.unidal.test.jetty.JettyServer;

import com.ctrip.xpipe.redis.core.CoreConfig;
import com.ctrip.xpipe.redis.core.DefaultCoreConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerLifecycleManager;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.util.MetaUpdateUtil;

/**
 * @author wenchao.meng
 *
 * Jun 20, 2016
 */
public class MetaServerPrepareResourcesAndStart extends JettyServer{

	private Logger logger = LoggerFactory.getLogger(MetaServerLifecycleManager.class);
	
	private int zkPort, serverPort;
	
	public MetaServerPrepareResourcesAndStart(int zkPort, int serverPort){
		this.zkPort = zkPort;
		this.serverPort = serverPort;
	}
	
	
	public ApplicationContext start(DcMeta meta) throws Exception {
		
		System.setProperty(MetaServerLifecycleManager.META_SERVER_START_KEY, "false");
		return start(connectToZk("127.0.0.1:" + zkPort), meta);
	}

	private XpipeMeta extractDcMeta(DcMeta meta) throws Exception {
		XpipeMeta xpipe = new XpipeMeta();
		DcMeta dc = new DcMeta();
		xpipe.addDc(dc);

		dc.setId(meta.getId());

		for (ClusterMeta cluster : meta.getClusters().values()) {
			ClusterMeta clusterClone = new ClusterMeta();
			dc.addCluster(clusterClone);

			clusterClone.setId(cluster.getId());

			for (ShardMeta shard : cluster.getShards().values()) {
				ShardMeta shardClone = new ShardMeta();
				clusterClone.addShard(shardClone);

				shardClone.setId(shard.getId());
				shardClone.setActiveDc(shard.getActiveDc());

				for (RedisMeta redis : shard.getRedises()) {
					RedisMeta redisClone = new RedisMeta();
					shardClone.addRedis(redisClone);

					redisClone.setIp(redis.getIp());
					redisClone.setMaster(redis.getMaster());
					redisClone.setPort(redis.getPort());
					redisClone.setShardActive(redis.getShardActive());

				}
			}
		}

		return xpipe;
	}

	public ApplicationContext start(CuratorFramework client, DcMeta dcMeta) throws Exception {
		
		setupZkNodes(client, dcMeta);
		
		XpipeMeta xpipeMeta = extractDcMeta(dcMeta);
		MetaUpdateUtil.updateMeta(client, xpipeMeta.toString());

		startServer();
		
		return setupResouces(dcMeta);
	}

	private ApplicationContext setupResouces(DcMeta dcMeta) throws Exception {
		
		ApplicationContext applicationContext = MetaServerLifecycleManager.getApplicationContext();
		
		logger.info("[setupResouces][set zkConnectionString]");
		DefaultCoreConfig coreConfig = applicationContext.getBean(DefaultCoreConfig.class);
		coreConfig.setZkConnectionString(String.format("%s:%d", "127.0.0.1", zkPort));
		
		logger.info("[setupResouces][start MetaServerLifecycleManager]");
		MetaServerLifecycleManager metaServerLifecycleManager = applicationContext.getBean(MetaServerLifecycleManager.class);
		metaServerLifecycleManager.startAll();
		
		return applicationContext;
		
	}

	private CuratorFramework connectToZk(String connectString) throws InterruptedException {
		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(3000);
		builder.connectString(connectString);
		builder.maxCloseWaitMs(3000);
		builder.namespace("xpipe");
		builder.retryPolicy(new RetryNTimes(3, 1000));
		builder.sessionTimeoutMs(5000);

		CuratorFramework client = builder.build();
		client.start();
		client.blockUntilConnected();

		return client;
	}

	private void setupZkNodes(CuratorFramework client, DcMeta dcMeta) throws Exception {
		CoreConfig config = new DefaultCoreConfig();
		MetaServerConfig metaServerConfig = new DefaultMetaServerConfig();

		for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
			for(ShardMeta shardMeta : clusterMeta.getShards().values()){
				String path = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), clusterMeta.getId(), shardMeta.getId());
				client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());
			}
		}
		String metaPath = metaServerConfig.getZkMetaStoragePath();
		client.newNamespaceAwareEnsurePath(metaPath).ensure(client.getZookeeperClient());
	}
	
	@Override
	protected int getServerPort() {
		return serverPort;
	}


	@Override
	protected String getContextPath() {
		return "/";
	}
	
	@Override
	protected File getWarRoot() {
		return new File("../redis-meta/src/main/webapp");
	}
}

