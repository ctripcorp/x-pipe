package com.ctrip.xpipe.redis.console.web.service;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.web.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcTbl;
import com.ctrip.xpipe.redis.console.web.model.DcTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcTblEntity;

@Service("dcClusterShardService")
public class DcClusterShardService {

private DcTblDao dcTblDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcTblDao = ContainerLoader.getDefaultContainer().lookup(DcTblDao.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<DcTbl> getByPK(String pk) throws DalException {
//		return clusterTblDao.findAllClusters(ClusterTblEntity.READSET_FULL);
		return dcTblDao.findAllDcs(DcTblEntity.READSET_FULL);
	}

}
