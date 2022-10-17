package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Set;

@Service
public class DcClusterShardServiceImpl extends AbstractConsoleService<DcClusterShardTblDao> implements DcClusterShardService {

	@Override
	public List<DcClusterShardTbl> findByShardId(long shardId) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllByShardId(shardId, DcClusterShardTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<DcClusterShardTbl> findAll() {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAll(DcClusterShardTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<DcClusterShardTbl> findAllDcClusterTblsByShard(long shardId) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllByShardId(shardId, DcClusterShardTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public DcClusterShardTbl findByPk(long dcClusterShardId) {
		return queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl doQuery() throws DalException {
				return dao.findByPK(dcClusterShardId, DcClusterShardTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public DcClusterShardTbl find(final long dcClusterId, final long shardId) {
		return queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl doQuery() throws DalException {
				return dao.findDcClusterShard(shardId, dcClusterId, DcClusterShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public DcClusterShardTbl find(final String dcName, final String clusterName, final String shardName) {
		return queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>(){
			@Override
			public DcClusterShardTbl doQuery() throws DalException {
				return dao.findDcClusterShardByName(dcName, clusterName, shardName, DcClusterShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<DcClusterShardTbl> find(String clusterName, String shardName) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>(){
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findClusterShardByName(clusterName, shardName, DcClusterShardTblEntity.READSET_FULL_WITH_DC_INFO);
			}
		});
	}

	@Override
	public List<DcClusterShardTbl> findAllByDcCluster(final long dcClusterId) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterId(dcClusterId, DcClusterShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<DcClusterShardTbl> findAllByDcCluster(final String dcName, final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllByDcClusterNames(dcName, clusterName, DcClusterShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public void updateDcClusterShard(DcClusterShardTbl dcClusterShardTbl) throws DalException{
		dao.updateByPK(dcClusterShardTbl, DcClusterShardTblEntity.UPDATESET_FULL);
	}

	@Override
	public List<DcClusterShardTbl> findAllByDcId(long dcId) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllByDcId(dcId, DcClusterShardTblEntity.READSET_CLUSTER_SHARD_REDIS_META_INFO);
			}
		});
	}

	@Override public List<DcClusterShardTbl> findAllByDcIdAndInClusterTypes(long dcId, Set<String> clusterTypes) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllByDcIdAndInClusterTypes(dcId, clusterTypes, DcClusterShardTblEntity.READSET_CLUSTER_SHARD_REDIS_META_INFO);
			}
		});
	}

	@Override
	public List<DcClusterShardTbl> findAllByClusterTypes(Set<String> clusterTypes) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllByClusterTypes(clusterTypes, DcClusterShardTblEntity.READSET_CLUSTER_SHARD_REDIS_META_INFO_WITHOUT_SENTINEL);
			}
		});
	}

	@Override
	public List<DcClusterShardTbl> findBackupDcShardsBySentinel(long sentinelId) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findBackupDcShardsBySentinel(sentinelId, DcClusterShardTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<DcClusterShardTbl> findAllShardsBySentinel(long sentinelId) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllShardsBySentinel(sentinelId, DcClusterShardTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<DcClusterShardTbl> findWithShardRedisBySentinel(long sentinelId) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findWithShardRedisBySentinel(sentinelId, DcClusterShardTblEntity.READSET_FULL_WITH_SHARD_REDIS);
			}
		});
	}

	@Override
	public void insertBatch(List<DcClusterShardTbl> dcClusterShardTbls) {
		queryHandler.handleBatchInsert(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return dao.insertBatch(dcClusterShardTbls.toArray(new DcClusterShardTbl[dcClusterShardTbls.size()]));
			}
		});
	}
}
