package com.ctrip.xpipe.redis.console.web.service;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.web.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.web.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTblEntity;

@Service("dcClusterShardService")
public class DcClusterShardService {

	private DcClusterTblDao dcClusterTblDao;

	@PostConstruct
	private void postConstruct() {
		try {
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public DcClusterTbl getByPK(String pk) throws DalException {
		return dcClusterTblDao.findByPK(1, DcClusterTblEntity.READSET_FULL);
	}

}
