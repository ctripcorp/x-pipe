package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public abstract class AbstractChangePrimaryDcAction implements ChangePrimaryDcAction{
	
	protected ExecutionLog executionLog = new ExecutionLog();
	
	protected DcMetaCache   dcMetaCache;
	
	protected CurrentMetaManager currentMetaManager;
	
	protected SentinelManager sentinelManager;

	public AbstractChangePrimaryDcAction(DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, SentinelManager sentinelManager) {
		this.dcMetaCache = dcMetaCache;
		this.currentMetaManager = currentMetaManager;
		this.sentinelManager = sentinelManager;
	}

	@Override
	public PrimaryDcChangeMessage changePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		return doChangePrimaryDc(clusterId, shardId, newPrimaryDc);
	}

	protected abstract PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc);

}
