package com.ctrip.xpipe.redis.meta.server.meta.impl;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultDcMetaManager;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
@Component
public class DefaultDcMetaCache extends AbstractLifecycleObservable implements DcMetaCache, Runnable, TopElement{

	public static String MEMORY_META_SERVER_DAO_KEY = "memory_meta_server_dao_file";
	
	@Autowired(required = false)
	private ConsoleService consoleService;
	
	@Autowired
	private MetaServerConfig metaServerConfig;
	
	private String currentDc = FoundationService.DEFAULT.getDataCenter();
	
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("Meta-Refresher"));
	private ScheduledFuture<?> future;
	
	private DcMetaManager dcMetaManager;

	public DefaultDcMetaCache(){
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		logger.info("[doInitialize][dc]{}", currentDc);
		this.dcMetaManager = loadMetaManager();
	}
	
	protected DcMetaManager loadMetaManager() {
		
		DcMetaManager dcMetaManager = null;
		if(consoleService != null){
			try{
				logger.info("[loadMetaManager][load from console]");
				DcMeta dcMeta = consoleService.getDcMeta(currentDc);
				dcMetaManager = DefaultDcMetaManager.buildFromDcMeta(dcMeta);
			}catch(ResourceAccessException e){
				logger.error("[loadMetaManager][consoleService]" + e.getMessage());
			}catch(Exception e){
				logger.error("[loadMetaManager][consoleService]", e);
			}
		}
		
		if(dcMetaManager == null){
			String fileName = System.getProperty(MEMORY_META_SERVER_DAO_KEY, "memory_meta_server_dao_file.xml");
			dcMetaManager = DefaultDcMetaManager.buildFromFile(currentDc, fileName);
		}

		logger.info("[loadMetaManager]{}", dcMetaManager);
				
		if(dcMetaManager == null){
			throw new IllegalArgumentException("[loadMetaManager][fail]");
		}
		return dcMetaManager;
	}



	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		future = scheduled.scheduleAtFixedRate(this, 0, metaServerConfig.getMetaRefreshMilli(), TimeUnit.MILLISECONDS);
		
	}

	@Override
	protected void doStop() throws Exception {
		
		future.cancel(true);
		super.doStop();
	}


	@Override
	public void run() {
		
		try{
			if(consoleService != null){
				
				//TODO update dcMeta, check for modification
				//DcMeta dcMeta = consoleService.getDcMeta(currentDc);
				//metaManager.update(dcMeta);
			}
		}catch(Throwable th){
			logger.error("[run]" + th.getMessage());
		}
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

	public DcMetaManager getDcMeta() {
		return this.dcMetaManager;
	}

	@Override
	public Set<String> getClusters() {
		return dcMetaManager.getClusters();
	}

	@Override
	public ClusterMeta getClusterMeta(String clusterId) {
		return dcMetaManager.getClusterMeta(clusterId);
	}

	@Override
	public KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta) {
		return dcMetaManager.getKeeperContainer(keeperMeta);
	}
}
