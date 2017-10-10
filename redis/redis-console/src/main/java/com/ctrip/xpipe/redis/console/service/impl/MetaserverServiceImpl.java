package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.MetaserverService;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;

@Service
public class MetaserverServiceImpl extends AbstractConsoleService<MetaserverTblDao> implements MetaserverService {

	@Override
	public List<MetaserverTbl> findAllByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<MetaserverTbl>>() {
			@Override
			public List<MetaserverTbl> doQuery() throws DalException {
				return dao.findByDcName(dcName, MetaserverTblEntity.READSET_FULL);
			}
    	});
	}

}
