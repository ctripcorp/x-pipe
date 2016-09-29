package com.ctrip.xpipe.redis.meta.server.keeper.elect;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Sep 9, 2016
 */
@Component
public class DefaultKeeperActiveElectAlgorithmManager implements KeeperActiveElectAlgorithmManager{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultKeeperActiveElectAlgorithmManager.class); 
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	@Override
	public KeeperActiveElectAlgorithm get(String clusterId, String shardId){
		
		boolean isActiveDc = dcMetaCache.isActiveDc(clusterId, shardId);
		if(isActiveDc){
			logger.debug("[get][active dc, use default]");
			return new DefaultKeeperActiveElectAlgorithm();
		}
		
		logger.debug("[get][backup dc, use UserDefinedPriorityKeeperActiveElectAlgorithm]");
		List<KeeperMeta> keepers = dcMetaCache.getShardKeepers(clusterId, shardId);
		return new UserDefinedPriorityKeeperActiveElectAlgorithm(keepers);
	}
	
	public void setDcMetaCache(DcMetaCache dcMetaCache) {
		this.dcMetaCache = dcMetaCache;
	}

}
