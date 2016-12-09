package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public abstract class AbstractChangePrimaryDcAction implements ChangePrimaryDcAction{

	@Override
	public PrimaryDcChangeMessage changePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		return doChangePrimaryDc(clusterId, shardId, newPrimaryDc);
	}

	protected abstract PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc);

}
