package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.lifecycle.Ordered;
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
	public LeaderElector createLeaderElector(ElectContext ctx) {
		return new DefaultLeaderElector(ctx, zkClient.get());
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}
	
}
