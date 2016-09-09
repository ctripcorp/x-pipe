package com.ctrip.xpipe.redis.meta.server.keeper.elect;


import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public class DefaultKeeperActiveElectAlgorithm extends AbstractActiveElectAlgorithm{

	@Override
	public KeeperMeta select(String clusterId, String shardId, List<KeeperMeta> toBeSelected){
		
		if(toBeSelected.size() > 0){
			KeeperMeta result = toBeSelected.get(0);
			result.setActive(true);
			return result;
		}
		return null;
	}

	

}
