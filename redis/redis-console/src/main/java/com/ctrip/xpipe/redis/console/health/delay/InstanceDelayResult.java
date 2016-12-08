package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.redis.console.health.BaseInstanceResult;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 11:28:31 PM
 */
public class InstanceDelayResult extends BaseInstanceResult<Void> {

	private String dcId;
	private boolean master;

	public InstanceDelayResult(String dcId, boolean master) {
		this.dcId = dcId;
		this.master = master;
	}

	public String getDcId() {
		return dcId;
	}

	public boolean isMaster() {
		return master;
	}

}
