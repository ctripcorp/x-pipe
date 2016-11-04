package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import java.util.concurrent.ScheduledExecutorService;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Nov 4, 2016
 */
public class PrimaryDcKeeperMasterChooser extends AbstractKeeperMasterChooser{

	public PrimaryDcKeeperMasterChooser(String clusterId, String shardId, DcMetaCache dcMetaCache,
			CurrentMetaManager currentMetaManager, ScheduledExecutorService scheduled) {
		super(clusterId, shardId, dcMetaCache, currentMetaManager, scheduled);
	}

	@Override
	protected Pair<String, Integer> chooseKeeperMaster() {
		
		if(!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)){
			
			logger.warn("[chooseKeeperMaster][current dc not primary]{}, {}", clusterId, shardId);
			try {
				stop();
			} catch (Exception e) {
				logger.error("[chooseKeeperMaster]", e);
			}
			return null;
		}
		
		//TODO setinel change event listen...
		for(RedisMeta redisMeta : dcMetaCache.getShardRedises(clusterId, shardId)){
			if(redisMeta.isMaster()){
				return new Pair<>(redisMeta.getIp(), redisMeta.getPort());
			}
		}
		return null;
	}

}
