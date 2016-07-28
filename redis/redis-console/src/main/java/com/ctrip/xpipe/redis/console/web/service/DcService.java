package com.ctrip.xpipe.redis.console.web.service;

import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.web.model.DcTbl;
import com.ctrip.xpipe.redis.console.web.model.DcTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcTblEntity;

/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@Service("dcClusterShardService")
public class DcService {

private DcTblDao dcTblDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcTblDao = ContainerLoader.getDefaultContainer().lookup(DcTblDao.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public List<DcTbl> getAllDcs() throws DalException {
		return dcTblDao.findAllDcs(DcTblEntity.READSET_FULL);
	}

}
