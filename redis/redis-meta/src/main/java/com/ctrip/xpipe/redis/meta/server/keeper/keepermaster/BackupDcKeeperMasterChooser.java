package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import java.util.concurrent.ScheduledExecutorService;

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
 * Nov 4, 2016
 */
public class BackupDcKeeperMasterChooser extends AbstractKeeperMasterChooser{

	private MetaServerMultiDcServiceManager metaServerMultiDcServiceManager;
	
	private MetaServerConfig metaServerConfig;

	public BackupDcKeeperMasterChooser(String clusterId, String shardId, MetaServerConfig metaServerConfig, MetaServerMultiDcServiceManager metaServerMultiDcServiceManager, 
			DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
		this(clusterId, shardId, metaServerConfig, metaServerMultiDcServiceManager, dcMetaCache, currentMetaManager, scheduled, DEFAULT_KEEPER_MASTER_CHECK_INTERVAL_SECONDS);
	}

	public BackupDcKeeperMasterChooser(String clusterId, String shardId, MetaServerConfig metaServerConfig, MetaServerMultiDcServiceManager metaServerMultiDcServiceManager,
			DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled, int checkIntervalSeconds) {
		super(clusterId, shardId, dcMetaCache, currentMetaManager, scheduled, checkIntervalSeconds);
		this.metaServerConfig = metaServerConfig;
		this.metaServerMultiDcServiceManager = metaServerMultiDcServiceManager;
	}

	@Override
	protected Pair<String, Integer> chooseKeeperMaster() {
		
		if(dcMetaCache.isCurrentDcPrimary(clusterId, shardId)){
			
			logger.warn("[chooseKeeperMaster][current dc become primary][stop]{}, {}", clusterId, shardId);
			try {
				stop();
			} catch (Exception e) {
				logger.error("[chooseKeeperMaster]", e);
			}
			return null;
		}
		
		return doChooseKeeperMaster();
	}

	private Pair<String, Integer> doChooseKeeperMaster() {
		
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
