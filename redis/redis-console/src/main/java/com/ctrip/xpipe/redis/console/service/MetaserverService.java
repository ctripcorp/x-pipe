package com.ctrip.xpipe.redis.console.service;


import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTblEntity;
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
@Service("metaserverService")
public class MetaserverService extends AbstractConsoleService<MetaserverTblDao>{
	
    public List<MetaserverTbl> findByDcName(final String dcName) {
    	return queryHandler.handleQuery(new DalQuery<List<MetaserverTbl>>() {
			@Override
			public List<MetaserverTbl> doQuery() throws DalNotFoundException, DalException {
				return dao.findByDcName(dcName, MetaserverTblEntity.READSET_FULL);
			}
    	});
    }
}
