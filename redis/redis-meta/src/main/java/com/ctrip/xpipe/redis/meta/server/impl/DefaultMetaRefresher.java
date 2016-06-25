package com.ctrip.xpipe.redis.meta.server.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultMetaOperation;
import com.ctrip.xpipe.redis.meta.server.MetaRefresher;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.rest.RestRequestClient;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jun 25, 2016
 */
@Component
public class DefaultMetaRefresher extends AbstractLifecycle implements MetaRefresher, Runnable, TopElement{
	
	
	@Autowired
	private MetaServerConfig metaServerConfig;
	
	@Autowired
	private ZkClient zkClient;
	
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("Meta-Refresher"));
	
	protected void doStart() throws Exception {
		
		scheduled.scheduleAtFixedRate(this, 0, metaServerConfig.getMetaRefreshMilli(), TimeUnit.MILLISECONDS);
		
	};
	
	@Override
	public void update() throws Exception {
		//TODO if not modified, not update
		String address = String.format("%s/api/v1/xpipe", metaServerConfig.getConsoleAddress());
		XpipeMeta result = RestRequestClient.get(address, XpipeMeta.class);
		logger.debug("[update]");
		new DefaultMetaOperation(zkClient.get()).update(result);
	}

	@Override
	public void run() {
		try{
			update();
		}catch(Throwable th){
			logger.error("[run]", th);
		}
	}

}
