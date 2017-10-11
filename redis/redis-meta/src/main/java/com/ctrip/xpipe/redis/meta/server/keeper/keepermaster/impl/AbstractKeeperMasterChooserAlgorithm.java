package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.KeeperMasterChooserAlgorithm;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public abstract class AbstractKeeperMasterChooserAlgorithm implements KeeperMasterChooserAlgorithm{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected DcMetaCache dcMetaCache;

	protected CurrentMetaManager currentMetaManager;

	protected String clusterId, shardId;
	
	protected ScheduledExecutorService scheduled;


	public AbstractKeeperMasterChooserAlgorithm(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {

		this.dcMetaCache = dcMetaCache;
		this.currentMetaManager = currentMetaManager;
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.scheduled = scheduled;
	}

	@Override
	public Pair<String, Integer> choose() {
		return doChoose();
	}

	protected abstract Pair<String, Integer> doChoose();

}
