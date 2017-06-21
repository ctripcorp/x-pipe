package com.ctrip.xpipe.redis.console.service.impl;

import java.util.List;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.KeepercontainerService;

@Service
public class KeepercontainerServiceImpl extends AbstractConsoleService<KeepercontainerTblDao> implements KeepercontainerService {

	@Override
	public KeepercontainerTbl find(final long id) {
		return queryHandler.handleQuery(new DalQuery<KeepercontainerTbl>() {
			@Override
			public KeepercontainerTbl doQuery() throws DalException {
				return dao.findByPK(id, KeepercontainerTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<KeepercontainerTbl> findAllByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
			@Override
			public List<KeepercontainerTbl> doQuery() throws DalException {
				return dao.findByDcName(dcName, KeepercontainerTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<KeepercontainerTbl> findAllActiveByDcName(String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
			@Override
			public List<KeepercontainerTbl> doQuery() throws DalException {
				return dao.findActiveByDcName(dcName, KeepercontainerTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<KeepercontainerTbl> findKeeperCount(String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<KeepercontainerTbl>>() {
			@Override
			public List<KeepercontainerTbl> doQuery() throws DalException {
				return dao.findKeeperCount(dcName, KeepercontainerTblEntity.READSET_KEEPER_COUNT);
			}
		});
	}


	protected Void update(KeepercontainerTbl keepercontainerTbl){

		return queryHandler.handleQuery(new DalQuery<Void>() {
			@Override
			public Void doQuery() throws DalException {
				dao.updateByPK(keepercontainerTbl, KeepercontainerTblEntity.UPDATESET_FULL);
				return null;
			}
		});
	}
}
