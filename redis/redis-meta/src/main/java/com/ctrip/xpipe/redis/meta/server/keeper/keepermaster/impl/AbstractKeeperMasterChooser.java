package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.MasterChooser;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 *         Nov 4, 2016
 */
public abstract class AbstractKeeperMasterChooser extends AbstractClusterShardPeriodicTask implements MasterChooser {

	public static int DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS = Integer
			.parseInt(System.getProperty("KEEPER_MASTER_CHECK_INTERVAL_SECONDS", "5"));

	protected int checkIntervalSeconds;

	public AbstractKeeperMasterChooser(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
									   CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
		this(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled,
				DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS);
	}

	public AbstractKeeperMasterChooser(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
									   CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, int checkIntervalSeconds) {
		super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
		this.checkIntervalSeconds = checkIntervalSeconds;
	}

	@Override
	protected void work() {
		String dcName = dcMetaCache.getPrimaryDc(clusterDbId, shardDbId);
		Pair<String, Integer> keeperMaster = chooseKeeperMaster();
		logger.debug("[doRun]cluster_{}, shard_{}, {}", clusterDbId, shardDbId, keeperMaster);
		Pair<String, Integer> currentMaster = currentMetaManager.getKeeperMaster(clusterDbId, shardDbId);
		if (keeperMaster == null || keeperMaster.equals(currentMaster)) {
			logger.debug("[doRun][new master null or equals old master]{}", keeperMaster);
			return;
		}
		logger.debug("[doRun][set]cluster_{}, shard_{}, {}", clusterDbId, shardDbId, keeperMaster);
		currentMetaManager.setKeeperMaster(clusterDbId, shardDbId, keeperMaster.getKey(), keeperMaster.getValue(), dcName);
	}

	@Override
	protected int getWorkIntervalSeconds() {
		return checkIntervalSeconds;
	}

	protected abstract Pair<String, Integer> chooseKeeperMaster();

}
