package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public class BackupDcKeeperMasterChooserAlgorithm extends AbstractKeeperMasterChooserAlgorithm{

	private MultiDcService multiDcService;

	public BackupDcKeeperMasterChooserAlgorithm(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, MultiDcService multiDcService) {
		super(clusterId, shardId, dcMetaCache, currentMetaManager);
		this.multiDcService = multiDcService;
	}
	

	@Override
	protected Pair<String, Integer> doChoose() {
		
		String dcName = dcMetaCache.getPrimaryDc(clusterId, shardId);
		
		KeeperMeta keeperMeta = multiDcService.getActiveKeeper(dcName, clusterId, shardId);
		logger.debug("[doChooseKeeperMaster]{}, {}, {}, {}", dcName, clusterId, shardId, keeperMeta);
		if(keeperMeta == null){
			return null;
		}
		return new Pair<>(keeperMeta.getIp(), keeperMeta.getPort());
	}
}
