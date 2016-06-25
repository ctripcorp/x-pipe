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

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.impl.AbstractCoreConfig;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultMetaOperation;

/**
 * @author wenchao.meng
 *
 * Jun 20, 2016
 */
public class MetaServerPrepareResourcesAndStart extends AbstractLifecycle {

	private Logger logger = LoggerFactory.getLogger(SpringComponentLifecycleManager.class);
	
	private MetaConsole metaConsole;
	private int serverPort;
	private String zkAddress;
	private DcMeta dcMeta;
	
	private ApplicationContext applicationContext;
	
	public MetaServerPrepareResourcesAndStart(String zkAddress, int serverPort, DcMeta dcMeta){
		
		this.zkAddress = zkAddress;
		this.serverPort = serverPort;
		this.dcMeta = dcMeta;
	}
	
	@Override
	public void doInitialize() throws Exception {
		metaConsole = new MetaConsole();
	}
	
	@Override
	public void  doStart() throws Exception {
		
		System.setProperty(SpringComponentLifecycleManager.SPRING_COMPONENT_START_KEY, "false");
		applicationContext = start(connectToZk(zkAddress), dcMeta);
	}
	

	@Override
	public void doStop() throws Exception {
		metaConsole.stopServer();
	}
	
	@Override
	protected void doDispose() throws Exception {
		metaConsole = null;
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
		new DefaultMetaOperation(client).update(xpipeMeta.toString());

		metaConsole.startServer();
		
		return setupResouces(dcMeta);
	}

	private ApplicationContext setupResouces(DcMeta dcMeta) throws Exception {
		
		ApplicationContext applicationContext = SpringComponentLifecycleManager.getApplicationContext();
		
		logger.info("[setupResouces][set zkConnectionString]");
		AbstractCoreConfig coreConfig = applicationContext.getBean(AbstractCoreConfig.class);
		coreConfig.setZkConnectionString(zkAddress);
		
		logger.info("[setupResouces][start MetaServerLifecycleManager]");
		SpringComponentLifecycleManager metaServerLifecycleManager = applicationContext.getBean(SpringComponentLifecycleManager.class);
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

		for(ClusterMeta clusterMeta : dcMeta.getClusters().values()){
			for(ShardMeta shardMeta : clusterMeta.getShards().values()){
				String path = MetaZkConfig.getKeeperLeaderLatchPath(clusterMeta.getId(), shardMeta.getId());
				client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());
			}
		}
		String metaPath = MetaZkConfig.getMetaRootPath();
		client.newNamespaceAwareEnsurePath(metaPath).ensure(client.getZookeeperClient());
	}
	
	
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
	

	public class MetaConsole extends JettyServer{

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
		
		@Override
		public void startServer() throws Exception {
			super.startServer();
		}
		
		@Override
		protected void stopServer() throws Exception {
			super.stopServer();
		}

	}
}

