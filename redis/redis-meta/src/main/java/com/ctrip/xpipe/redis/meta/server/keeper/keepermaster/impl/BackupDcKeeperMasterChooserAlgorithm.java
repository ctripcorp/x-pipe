package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
public class BackupDcKeeperMasterChooserAlgorithm extends AbstractKeeperMasterChooserAlgorithm{
	
	private MetaServerMultiDcServiceManager metaServerMultiDcServiceManager;
	
	private MetaServerConfig metaServerConfig;

	public BackupDcKeeperMasterChooserAlgorithm(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, MetaServerConfig metaServerConfig, MetaServerMultiDcServiceManager metaServerMultiDcServiceManager) {
		super(clusterId, shardId, dcMetaCache, currentMetaManager);
		this.metaServerConfig = metaServerConfig;
		this.metaServerMultiDcServiceManager = metaServerMultiDcServiceManager;
	}
	

	@Override
	protected Pair<String, Integer> doChoose() {
		
		String dcName = dcMetaCache.getPrimaryDc(clusterId, shardId);
		DcInfo dcInfo = metaServerConfig.getDcInofs().get(dcName);
		if(dcInfo == null){
			logger.error("[doChooseKeeperMaster][dc info null]{}", dcName);
			return null;
		}
		
		MetaServerMultiDcService metaServerMultiDcService = metaServerMultiDcServiceManager.getOrCreate(dcInfo.getMetaServerAddress());
		KeeperMeta keeperMeta = metaServerMultiDcService.getActiveKeeper(clusterId, shardId);
		logger.debug("[doChooseKeeperMaster]{}, {}, {}, {}", dcName, clusterId, shardId, keeperMeta);
		if(keeperMeta == null){
			return null;
		}
		return new Pair<>(keeperMeta.getIp(), keeperMeta.getPort());
	}
}
