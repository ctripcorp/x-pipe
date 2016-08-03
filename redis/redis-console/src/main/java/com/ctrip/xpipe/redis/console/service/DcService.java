package com.ctrip.xpipe.redis.console.service;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Readset;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.web.model.DcTbl;
import com.ctrip.xpipe.redis.console.web.model.DcTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcTblEntity;

/**
 * @author shyin
 *
 * Jul 28, 2016
 */
@Service("dcService")
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

	public List<String> getAllDcs() throws DalException {
		List<String> dcIds = new ArrayList<String>(10);
		
		for(DcTbl dc : dcTblDao.findAllDcs(new Readset<DcTbl>(DcTblEntity.DC_ID)) ) {
			dcIds.add(dc.getDcId());
		}
			
		return dcIds;
	}

}
