package com.ctrip.xpipe.redis.meta.server.keeper.impl;


import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public class DefaultLeaderElectAlgorithm extends AbstractLeaderElectAlgorithm{

	@Override
	public KeeperMeta select(List<KeeperMeta> toBeSelected) throws Exception {
		
		if(toBeSelected.size() > 0){
			KeeperMeta result = toBeSelected.get(0);
			result.setActive(true);
			return result;
		}
		return null;
	}

	

}
