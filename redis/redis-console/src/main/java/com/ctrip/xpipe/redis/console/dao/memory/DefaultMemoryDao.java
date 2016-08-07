package com.ctrip.xpipe.redis.console.dao.memory;


import java.util.LinkedList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.console.dao.ConsoleDao;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaException;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@Component
public class DefaultMemoryDao extends DefaultXpipeMetaManager implements ConsoleDao{
	
	public DefaultMemoryDao() {
	}
	
	@Override
	public XpipeMeta getXpipeMeta() {
		return this.xpipeMeta;
	}


	@Override
	public boolean updateActiveDc(String clusterId, String activeDc) throws MetaException {
		
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
