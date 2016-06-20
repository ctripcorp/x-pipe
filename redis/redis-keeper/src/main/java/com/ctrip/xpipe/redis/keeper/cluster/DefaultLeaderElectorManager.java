/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.cluster;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.zk.ZkClient;

/**
 * @author marsqing
 *
 *         Jun 17, 2016 4:53:49 PM
 */
@Component
public class DefaultLeaderElectorManager extends AbstractLifecycle implements TopElement, LeaderElectorManager {

	@Autowired
	private ZkClient zkClient;

	@Override
	public LeaderElector createLeaderElector(ElectContext ctx) {
		return new DefaultLeaderElector(ctx, zkClient.get());
	}
	
	public void setZkClient(ZkClient zkClient) {
		this.zkClient = zkClient;
	}

}
