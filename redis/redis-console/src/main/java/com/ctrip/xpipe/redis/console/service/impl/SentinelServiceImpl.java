package com.ctrip.xpipe.redis.console.service.impl;

import java.util.List;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.SetinelTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.SentinelService;

@Service
public class SentinelServiceImpl extends AbstractConsoleService<SetinelTblDao> implements SentinelService {

	@Override
	public List<SetinelTbl> findAllByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return dao.findByDcName(dcName, SetinelTblEntity.READSET_FULL);
			}
    	});
	}
	
	@Override
	public SetinelTbl find(final long id) {
		return queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
			@Override
			public SetinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SetinelTblEntity.READSET_FULL);
			}
		});
	}

}
