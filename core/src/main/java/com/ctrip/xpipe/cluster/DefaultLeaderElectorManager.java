package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author marsqing
 *
 *         Jun 17, 2016 4:53:49 PM
 */
public class DefaultLeaderElectorManager extends AbstractLifecycle implements LeaderElectorManager, TopElement {
	
	
	private ZkClient zkClient;
	
	
	public DefaultLeaderElectorManager(ZkClient zkClient) {
		this.zkClient = zkClient;
	}
	
	@Override
	protected void doInitialize() throws Exception {
		zkClient.initialize();
	}

	@Override
	protected void doStart() throws Exception {
		zkClient.start();
	}
	
	@Override
	protected void doStop() throws Exception {
		zkClient.stop();
	}

	
	@Override
	protected void doDispose() throws Exception {
		zkClient.dispose();
	}

	@Override
	public LeaderElector createLeaderElector(ElectContext ctx) {
		return new DefaultLeaderElector(ctx, zkClient.get());
	}
	
}
