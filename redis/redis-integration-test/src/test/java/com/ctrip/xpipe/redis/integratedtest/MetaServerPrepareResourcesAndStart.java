package com.ctrip.xpipe.redis.integratedtest;


import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCache;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.unidal.test.jetty.JettyServer;

import java.io.File;

/**
 * TODO to fix use spring boot
 * @author wenchao.meng
 *
 * Jun 20, 2016
 */
public class MetaServerPrepareResourcesAndStart extends AbstractLifecycle {

	private Logger logger = LoggerFactory.getLogger(MetaServerPrepareResourcesAndStart.class);
	
	private String integratedTestFile;
	private MetaJettyServer metaJettyServer;
	private int serverPort;
	private String zkAddress;
	private DcMeta dcMeta;
	
	private ApplicationContext applicationContext;
	
	public MetaServerPrepareResourcesAndStart(String integratedTestFile, String zkAddress, int serverPort, DcMeta dcMeta){
		
		this.integratedTestFile = integratedTestFile;
		this.zkAddress = zkAddress;
		this.serverPort = serverPort;
		this.dcMeta = dcMeta;
	}
	
	@Override
	public void doInitialize() throws Exception {
		metaJettyServer = new MetaJettyServer();
	}
	
	@Override
	public void  doStart() throws Exception {
		
		System.setProperty(DefaultDcMetaCache.MEMORY_META_SERVER_DAO_KEY, integratedTestFile);
		applicationContext = start(connectToZk(zkAddress), dcMeta);
	}
	

	@Override
	public void doStop() throws Exception {
		metaJettyServer.stopServer();
	}
	
	@Override
	protected void doDispose() throws Exception {
		metaJettyServer = null;
	}

	public ApplicationContext start(CuratorFramework client, DcMeta dcMeta) throws Exception {
		
		setupZkNodes(client, dcMeta);
		

		metaJettyServer.startServer();
		
		return setupResouces(dcMeta);
	}

	private ApplicationContext setupResouces(DcMeta dcMeta) throws Exception {
		
		ApplicationContext applicationContext = SpringComponentLifecycleManager.getApplicationContext();
		
		logger.info("[setupResouces][set zkConnectionString]");
		ZkClient zkClient = applicationContext.getBean(ZkClient.class);
		zkClient.setZkAddress(zkAddress);
		
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
				client.createContainers(path);
			}
		}
		String metaPath = MetaZkConfig.getMetaRootPath();
		client.createContainers(metaPath);
	}
	
	
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
	

	public class MetaJettyServer extends JettyServer{

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

