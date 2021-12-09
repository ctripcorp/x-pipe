package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public class BackupDcKeeperMasterChooserAlgorithm extends AbstractKeeperMasterChooserAlgorithm{

	private MultiDcService multiDcService;

	public BackupDcKeeperMasterChooserAlgorithm(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, MultiDcService multiDcService, ScheduledExecutorService scheduled) {
		super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, scheduled);
		this.multiDcService = multiDcService;
	}
	

	@Override
	protected Pair<String, Integer> doChoose() {
		
		String dcName = dcMetaCache.getPrimaryDc(clusterDbId, shardDbId);
		
		KeeperMeta keeperMeta = multiDcService.getActiveKeeper(dcName, clusterDbId, shardDbId);
		logger.debug("[doChooseKeeperMaster]{}, {}, {}, {}", dcName, clusterDbId, shardDbId, keeperMeta);
		if(keeperMeta == null){
			return null;
		}
		return new Pair<>(keeperMeta.getIp(), keeperMeta.getPort());
	}
}
