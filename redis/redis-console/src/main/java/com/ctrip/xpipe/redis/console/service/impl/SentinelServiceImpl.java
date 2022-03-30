package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.SentinelTbl;
import com.ctrip.xpipe.redis.console.model.SentinelTblDao;
import com.ctrip.xpipe.redis.console.model.SentinelTblEntity;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Random;

@Service
public class SentinelServiceImpl extends AbstractConsoleService<SentinelTblDao> implements SentinelService {

	@Override
	public List<SentinelTbl> findAll() {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findAll(SentinelTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<SentinelTbl> findAllWithDcName() {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findAllWithDcName(SentinelTblEntity.READSET_SENTINEL_DC_NAME);
			}
		});
	}

	@Override
	public List<SentinelTbl> findAllByDcName(final String dcName) {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findByDcName(dcName, SentinelTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<SentinelTbl> findBySentinelGroupId(long sentinelGroupId) {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findBySentinelGroupId(sentinelGroupId, SentinelTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<SentinelTbl> findBySentinelGroupIdDeleted(long sentinelGroupId) {
		return queryHandler.handleQuery(new DalQuery<List<SentinelTbl>>() {
			@Override
			public List<SentinelTbl> doQuery() throws DalException {
				return dao.findBySentinelGroupIdDeleted(sentinelGroupId, SentinelTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public SentinelTbl findByIpPort(String ip, int port) {
		return queryHandler.handleQuery(new DalQuery<SentinelTbl>() {
			@Override
			public SentinelTbl doQuery() throws DalException {
				return dao.findByIpPort(ip, port, SentinelTblEntity.READSET_FULL);
			}
		});
	}

	protected SetinelTbl random(List<SetinelTbl> setinels) {

		Random random = new Random();

		int index = random.nextInt(setinels.size());

		return setinels.get(index);
	}

	@Override
	public SentinelTbl insert(SentinelTbl sentinelTbl) {

		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.insert(sentinelTbl);
			}
		});

		return sentinelTbl;
	}


	@Override
	public void delete(long id) {
		SentinelTbl sentinelTbl = queryHandler.handleQuery(new DalQuery<SentinelTbl>() {
			@Override
			public SentinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SentinelTblEntity.READSET_FULL);
			}
		});
		if (sentinelTbl == null) {
			throw new IllegalArgumentException(String.format("sentinel with id:%d not exist", id));
		}
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.deleteSentinel(sentinelTbl,SentinelTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void reheal(long id) {
		SentinelTbl setinelTbl = queryHandler.handleQuery(new DalQuery<SentinelTbl>() {
			@Override
			public SentinelTbl doQuery() throws DalException {
				return dao.findByPK(id, SentinelTblEntity.READSET_FULL);
			}
		});
		setinelTbl.setDeleted(0);
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(setinelTbl, SentinelTblEntity.UPDATESET_FULL);
			}
		});
	}


	@Override
	public void updateByPk(SentinelTbl sentinelTbl) {
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(sentinelTbl, SentinelTblEntity.UPDATESET_FULL);
			}
		});
	}

}
