package com.ctrip.xpipe.redis.console.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.SetinelTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.SentinelService;

@Service
public class SentinelServiceImpl extends AbstractConsoleService<SetinelTblDao> implements SentinelService {

	private DcClusterShardTblDao dcClusterShardTblDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Dao construct failed.", e);
		}
	}
	
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

	@Override
	public Map<Long, SetinelTbl> findByShard(long shardId) {
		List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dcClusterShardTblDao.findAllByShardId(shardId, DcClusterShardTblEntity.READSET_FULL);
			}
		});
		
		Map<Long, SetinelTbl> res = new HashMap<>(dcClusterShards.size());
		for(DcClusterShardTbl dcClusterShard : dcClusterShards) {
			SetinelTbl sentinel = queryHandler.handleQuery(new DalQuery<SetinelTbl>() {
				@Override
				public SetinelTbl doQuery() throws DalException {
					return dao.findByPK(dcClusterShard.getSetinelId(), SetinelTblEntity.READSET_FULL);
				}
			});
			if(null != sentinel) {
				res.put(sentinel.getDcId(), sentinel);
			}
		}
		return res;
	}


}
