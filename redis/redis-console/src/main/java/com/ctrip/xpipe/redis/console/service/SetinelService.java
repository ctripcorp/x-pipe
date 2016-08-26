package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.SetinelTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import java.util.List;


/**
 * @author shyin
 * 
 * Aug 20, 2016
 */
@Service("setinelService")
public class SetinelService extends AbstractConsoleService<SetinelTblDao>{
	
    public List<SetinelTbl> findByDcName(final String dcName) {
    	return queryHandler.handleQuery(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalNotFoundException, DalException {
				return dao.findByDcName(dcName, SetinelTblEntity.READSET_FULL);
			}
    	});
    }
}
