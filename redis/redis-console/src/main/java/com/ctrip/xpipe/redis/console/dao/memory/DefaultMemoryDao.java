package com.ctrip.xpipe.redis.console.dao.memory;


import java.util.LinkedList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.console.dao.ConsoleDao;
import com.ctrip.xpipe.redis.core.dao.DaoException;
import com.ctrip.xpipe.redis.core.dao.memory.DefaultMemoryMetaDao;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@Component
public class DefaultMemoryDao extends DefaultMemoryMetaDao implements ConsoleDao{

	@Override
	public XpipeMeta getXpipeMeta() {
		return this.xpipeMeta;
	}


	@Override
	public boolean updateActiveDc(String clusterId, String activeDc) throws DaoException {
		
		logger.info("[updateActiveDc]{}, {}", clusterId, activeDc);
		String currentActive = getActiveDc(clusterId);
		if(currentActive.equals(activeDc)){
			logger.info("[updateActiveDc][not changed]{}, {}", clusterId, activeDc);
			return false;
		}
		
		for(ClusterMeta clusterMeta : getClusterMetaInAllDc(clusterId)){
			clusterMeta.setActiveDc(activeDc);
		}
		return true;
	}

	protected List<ClusterMeta> getClusterMetaInAllDc(String clusterId) {
		
		List<ClusterMeta> result = new LinkedList<>();
		for(DcMeta dcMeta : xpipeMeta.getDcs().values()){
			ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterId);
			if(clusterMeta == null){
				continue;
			}
			result.add(clusterMeta);
		}
		return result;
	}
}
