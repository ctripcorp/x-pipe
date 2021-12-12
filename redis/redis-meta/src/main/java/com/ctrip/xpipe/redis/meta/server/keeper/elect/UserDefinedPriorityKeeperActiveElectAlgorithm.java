package com.ctrip.xpipe.redis.meta.server.keeper.elect;


import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public class UserDefinedPriorityKeeperActiveElectAlgorithm extends AbstractActiveElectAlgorithm{
	
	private List<KeeperMeta> userDefinedPriority;
	
	public UserDefinedPriorityKeeperActiveElectAlgorithm(List<KeeperMeta> userDefinedPriority) {
		this.userDefinedPriority = userDefinedPriority;
	}

	@Override
	public KeeperMeta select(Long clusterDbId, Long shardDbId, List<KeeperMeta> toBeSelected) {

		if(toBeSelected.size() == 0){
			return null;
		}
		
		for(KeeperMeta keeperMeta : userDefinedPriority){
			for(KeeperMeta select : toBeSelected){
				if(MetaUtils.same(keeperMeta, select)){
					return select;
				}
			}
		}
		
		logger.warn("[select][no keeper in given list, use first]{}, {}", userDefinedPriority, toBeSelected);
		return toBeSelected.get(0);
	}
}
