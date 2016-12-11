package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Dec 11, 2016
 */
public class BecomePrimaryAction extends AbstractChangePrimaryDcAction{

	private NewMasterChooser newMasterChooser;

	public BecomePrimaryAction(DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, SentinelManager sentinelManager, NewMasterChooser newMasterChooser) {
		super(dcMetaCache, currentMetaManager, sentinelManager);
		this.newMasterChooser = newMasterChooser;
	}

	
	@Override
	protected PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc) {
		
		doChangeMetaCache(clusterId, shardId, newPrimaryDc);
		
		RedisMeta newMaster = chooseNewMaster();
		
		makeRedisesOk();
		
		makeKeepersOk();
		
		addSentinel();
		
		return null;
	}

	private void addSentinel() {
		
	}

	private void makeKeepersOk() {
		
	}

	private void makeRedisesOk() {
		
	}

	private RedisMeta chooseNewMaster() {
		return null;
	}

	private void doChangeMetaCache(String clusterId, String shardId, String newPrimaryDc) {
		
	}

}
