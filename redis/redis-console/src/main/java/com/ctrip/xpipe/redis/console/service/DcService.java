package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import java.util.List;

/**
 * @author shyin
 *
 * Aug 20, 2016
 */
@Service
public class DcService extends AbstractConsoleService<DcTblDao>{
   
    public DcTbl load(final String dcName) {
    	return queryHandler.handleQuery(new DalQuery<DcTbl>() {
			@Override
			public DcTbl doQuery() throws DalException {
				return dao.findDcByDcName(dcName, DcTblEntity.READSET_FULL);
			}
    	});
    }

    public DcTbl load(final long dcId) {
    	return queryHandler.handleQuery(new DalQuery<DcTbl>(){
			@Override
			public DcTbl doQuery() throws DalException {
				return dao.findByPK(dcId, DcTblEntity.READSET_FULL);
			}
    	});
    }

    public List<DcTbl> findAllDcs() {
    	return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllDcs(DcTblEntity.READSET_FULL);
			}
    	});
    }
    
    public List<DcTbl> findAllDcNames() {
    	return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllDcs(DcTblEntity.READSET_NAME);
			}
    	});
    }

    public List<DcTbl> findClusterRelatedDc(final String clusterName) {
    	return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findClusterRelatedDc(clusterName, DcTblEntity.READSET_FULL);
			}
    	});
    }
    
    public List<DcTbl> findAllDetails(final String dcName) {
    	return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findDcDetailsByDcName(dcName, DcTblEntity.READSET_FULL_ALL);
			}
    	});
    }
    
    public List<DcTbl> findAllActiveKeepers() {
    	return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllActiveKeeper(DcTblEntity.READSET_FULL_ALL);
			}
    	});
    }

}
