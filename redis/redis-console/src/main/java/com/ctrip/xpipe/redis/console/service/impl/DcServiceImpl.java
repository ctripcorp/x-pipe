package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DcServiceImpl extends AbstractConsoleService<DcTblDao> implements DcService {

	@Override
	public DcTbl find(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<DcTbl>() {
			@Override
			public DcTbl doQuery() throws DalException {
				return dao.findDcByDcName(dcName, DcTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public DcTbl find(final long dcId) {
		return queryHandler.handleQuery(new DalQuery<DcTbl>(){
			@Override
			public DcTbl doQuery() throws DalException {
				return dao.findByPK(dcId, DcTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public String getDcName(long dcId) {
		DcTbl dcTbl = find(dcId);
		if(dcTbl == null){
			throw new IllegalArgumentException("dc for dcid not found:" + dcId);
		}
		return dcTbl.getDcName();
	}

	@Override
	public List<DcTbl> findAllDcs() {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllDcs(DcTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<DcTbl> findAllDcNames() {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllDcs(DcTblEntity.READSET_NAME);
			}
    	});
	}

	@Override
	public List<DcTbl> findAllDcBasic() {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllDcs(DcTblEntity.READSET_BASIC);
			}
    	});
	}

	@Override
	public List<DcTbl> findClusterRelatedDc(final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findClusterRelatedDc(clusterName, DcTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<DcTbl> findAllDetails(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findDcDetailsByDcName(dcName, DcTblEntity.READSET_FULL_ALL);
			}
    	});
	}

	@Override
	public List<DcTbl> findAllActiveKeepers() {
		return queryHandler.handleQuery(new DalQuery<List<DcTbl>>() {
			@Override
			public List<DcTbl> doQuery() throws DalException {
				return dao.findAllActiveKeeper(DcTblEntity.READSET_FULL_ALL);
			}
    	});
	}

	@Override
	public DcTbl findByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<DcTbl>() {
			@Override
			public DcTbl doQuery() throws DalException {
				return dao.findDcByDcName(dcName, DcTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public Map<Long, String> dcNameMap() {

		List<DcTbl> allDcs = findAllDcs();
		Map<Long, String> result = new HashMap<>();

		allDcs.forEach(dcTbl -> result.put(dcTbl.getId(), dcTbl.getDcName()));
		return result;
	}

}
