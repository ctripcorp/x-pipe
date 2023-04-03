package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.utils.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class DcClusterShardServiceImpl extends AbstractConsoleService<DcClusterShardTblDao> implements DcClusterShardService {

	@Autowired
	private ClusterService clusterService;

	@Autowired
	private DcClusterService dcClusterService;

	@Autowired
	private DcClusterShardService dcClusterShardService;

	@Autowired
	private ShardService shardService;

	@Autowired
	private RedisService redisService;

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

	@Override
	public List<DcClusterShardTbl> findAllByDcIdAndInClusterTypes(List<DcTbl> allDcsTblList, Set<String> clusterTypes) {

		List<ClusterTbl> allClusterTbls = clusterService.findAllClustersWithOrgInfo();
		List<DcClusterTbl> allDcClusterTbls = dcClusterService.findAllDcClusters();
		List<DcClusterShardTbl> allDcClusterShardTbls = dcClusterShardService.findAll();
		List<ShardTbl> allShardTbls = shardService.findAll();
		List<RedisTbl> allRedisTbls = redisService.findByRole(XPipeConsoleConstant.ROLE_REDIS);
		allRedisTbls.addAll(redisService.findByRole(XPipeConsoleConstant.ROLE_KEEPER));

		Map<Long, ClusterTbl> clusterId2ClusterTbl = allClusterTbls.stream().filter(clusterTbl -> clusterTypes.contains(clusterTbl.getClusterType().toUpperCase())).collect(Collectors.toMap(ClusterTbl::getId, clusterTbl -> clusterTbl));
		Map<Long, List<DcClusterTbl>> dcId2DcClusterTbls = new HashMap<>();
		for (DcClusterTbl dcClusterTbl : allDcClusterTbls) {
			List<DcClusterTbl> dcClusterTbls = MapUtils.getOrCreate(dcId2DcClusterTbls, dcClusterTbl.getDcId(), LinkedList::new);
			dcClusterTbls.add(dcClusterTbl);
		}

		Map<Long, List<DcClusterShardTbl>> dcClusterId2DcClusterShardTbls = new HashMap<>();
		for (DcClusterShardTbl dcClusterShardTbl : allDcClusterShardTbls) {
			List<DcClusterShardTbl> dcClusterShardTbls = MapUtils.getOrCreate(dcClusterId2DcClusterShardTbls, dcClusterShardTbl.getDcClusterId(), LinkedList::new);
			dcClusterShardTbls.add(dcClusterShardTbl);
		}

		Map<Long, ShardTbl> shardTblMap = allShardTbls.stream().collect(Collectors.toMap(ShardTbl::getId, shardTbl -> shardTbl));
		Map<Long, List<RedisTbl>> dcClusterShardId2RedisTbls = new HashMap<>();
		for (RedisTbl redisTbl : allRedisTbls) {
			List<RedisTbl> redisTbls = MapUtils.getOrCreate(dcClusterShardId2RedisTbls, redisTbl.getDcClusterShardId(), LinkedList::new);
			redisTbls.add(redisTbl);
		}

		return buildDcClusterShardTbls(allDcsTblList, clusterId2ClusterTbl, dcId2DcClusterTbls, dcClusterId2DcClusterShardTbls, shardTblMap, dcClusterShardId2RedisTbls);
	}

	List<DcClusterShardTbl> buildDcClusterShardTbls(List<DcTbl> allDcsTblList, Map<Long, ClusterTbl> clusterId2ClusterTbl,
													Map<Long, List<DcClusterTbl>> dcId2DcClusterTbls, Map<Long, List<DcClusterShardTbl>> dcClusterId2DcClusterShardTbls,
													Map<Long, ShardTbl> shardTblMap, Map<Long, List<RedisTbl>> dcClusterShardId2RedisTbls) {
		List<DcClusterShardTbl> dcClusterShardTbls = new LinkedList<>();
		for (DcTbl dcTbl : allDcsTblList) {

			List<DcClusterTbl> dcClusterTbls = dcId2DcClusterTbls.get(dcTbl.getId());
			if (dcClusterTbls == null)
				continue;

			for (DcClusterTbl dcClusterTbl : dcClusterTbls) {
				List<DcClusterShardTbl> dcClusterShardTbls1 = dcClusterId2DcClusterShardTbls.get(dcClusterTbl.getDcClusterId());
				if (dcClusterShardTbls1 == null)
					continue;

				ClusterTbl clusterTbl = clusterId2ClusterTbl.get(dcClusterTbl.getClusterId());
				if (clusterTbl == null)
					continue;

				List<DcClusterShardTbl> dcClusterShardTblsWithRedis = new LinkedList<>();

				for (DcClusterShardTbl dcClusterShardTbl : dcClusterShardTbls1) {
					ShardTbl shardTbl = shardTblMap.get(dcClusterShardTbl.getShardId());
					if (shardTbl == null)
						continue;

					List<RedisTbl> redisTbls = dcClusterShardId2RedisTbls.get(dcClusterShardTbl.getDcClusterShardId());
					if (redisTbls == null)
						continue;

					redisTbls.forEach(redisTbl -> {
						DcClusterShardTbl local = new DcClusterShardTbl().
								setDcClusterShardId(dcClusterShardTbl.getDcClusterShardId()).
								setDcClusterId(dcClusterShardTbl.getDcClusterId()).
								setShardId(dcClusterShardTbl.getShardId()).
								setSetinelId(dcClusterShardTbl.getSetinelId());
						local.setDcClusterInfo(dcClusterTbl);
						local.setClusterInfo(clusterTbl);
						local.setShardInfo(shardTbl);
						local.setRedisInfo(redisTbl);
						dcClusterShardTblsWithRedis.add(local);
					});
				}

				dcClusterShardTbls.addAll(dcClusterShardTblsWithRedis);
			}
		}
		return dcClusterShardTbls;
	}


	@Override
	public List<DcClusterShardTbl> findAllByClusterTypes(Set<String> clusterTypes) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dao.findAllByClusterTypes(clusterTypes, DcClusterShardTblEntity.READSET_DC_CLUSTER_SHARD_REDIS_META_INFO);
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
