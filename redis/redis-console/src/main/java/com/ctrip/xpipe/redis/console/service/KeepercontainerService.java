package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import java.util.List;


/**
 * @author shyin
 *
 * Aug 20, 2016
 */
@Service("keepercontainerService")
public class KeepercontainerService extends AbstractConsoleService<KeepercontainerTblDao>{

    public List<KeepercontainerTbl> findByDcName(final String dcName) {
    	return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
			@Override
			public List<KeepercontainerTbl> doQuery() throws DalException {
				return dao.findByDcName(dcName, KeepercontainerTblEntity.READSET_FULL);
			}
    	});
    }
    
    public KeepercontainerTbl load(final Long id) {
    	return queryHandler.handleQuery(new DalQuery<KeepercontainerTbl>() {
			@Override
			public KeepercontainerTbl doQuery() throws DalException {
				return dao.findByPK(id, KeepercontainerTblEntity.READSET_FULL);
			}
    	});
    }
}
