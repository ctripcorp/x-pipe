package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.google.common.collect.Sets;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.*;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Repository
public class ShardDao extends AbstractXpipeConsoleDAO{
	private ClusterTblDao clusterTblDao;
	private DcClusterTblDao dcClusterTblDao;
	private ShardTblDao shardTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	
	@Autowired
	private DcClusterShardDao dcClusterShardDao;

	private Random random = new Random();

	@PostConstruct
	private void postConstruct() {
		try {
			clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}

	public ShardTbl createShard(String clusterName, ShardTbl shard, Map<Long, SentinelGroupModel> sentinels) throws DalException{
		// shard basic
		shard.setSetinelMonitorName(shard.getShardName().trim());
		validateShard(clusterName, shard);
		return insertShard(clusterName, shard, sentinels);
	}

	public List<ShardTbl> queryAllShardsByClusterName(String clusterName) {
        return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>(){
            @Override
            public List<ShardTbl> doQuery() throws DalException {
                return shardTblDao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
            }
        });
    }

    public Set<String> queryAllShardMonitorNames() {
        List<ShardTbl> shards = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
            @Override
            public List<ShardTbl> doQuery() throws DalException {
                return shardTblDao.queryAllMonitorNames(ShardTblEntity.READSET_MONITOR_NAME);
            }
        });
        Set<String> monitorNames = Sets.newHashSetWithExpectedSize(shards.size());
        shards.forEach(shardTbl -> monitorNames.add(shardTbl.getSetinelMonitorName()));

        return monitorNames;
    }

	@DalTransaction
	public void deleteShardsBatch(List<ShardTbl> shards) throws DalException {
		if(null == shards || shards.isEmpty()) {
			logger.warn("[deleteShardsBatch] Empty shards: {}", shards);
			return;
		}
		
		List<DcClusterShardTbl> relatedDcClusterShards = new LinkedList<DcClusterShardTbl>();
		for(final ShardTbl shard : shards) {
			List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
				@Override
				public List<DcClusterShardTbl> doQuery() throws DalException {
					return dcClusterShardTblDao.findAllByShardId(shard.getId(), DcClusterShardTblEntity.READSET_FULL);
				}
			});
			
			if(null != dcClusterShards) {
				relatedDcClusterShards.addAll(dcClusterShards);
			}
		}
		dcClusterShardDao.deleteDcClusterShardsBatch(relatedDcClusterShards);
		
		for(ShardTbl shard : shards) {
			shard.setShardName(generateDeletedName(shard.getShardName()));
		}
		queryHandler.handleBatchDelete(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return shardTblDao.deleteShardsBatch(shards.toArray(new ShardTbl[shards.size()]),
						ShardTblEntity.UPDATESET_FULL);
			}
		}, true);
	}
	
	@DalTransaction
	public void deleteShardsBatch(final ShardTbl shard) throws DalException {
		if(null == shard) throw new DalException("Null cannot be deleted.");
		
		List<DcClusterShardTbl> relatedDcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dcClusterShardTblDao.findAllByShardId(shard.getId(), DcClusterShardTblEntity.READSET_FULL);
			}
		});
		if(null != relatedDcClusterShards) {
			dcClusterShardDao.deleteDcClusterShardsBatch(relatedDcClusterShards);
		}
		
		ShardTbl proto = shard;
		proto.setShardName(generateDeletedName(shard.getShardName()));

		queryHandler.handleDelete(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return shardTblDao.deleteShard(proto, ShardTblEntity.UPDATESET_FULL);
			}
		}, true);
	}
	
	public void validateShard(final String clusterName, ShardTbl shard) throws DalException {
		// validate monitor name
		if (!shard.getShardName().equals(shard.getSetinelMonitorName())) {
			throw new BadRequestException("Monitor name should be exact same with shard name");
		}

		// validate shard name
		List<ShardTbl> shards = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>(){
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return shardTblDao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
			}
		});
		if(null == shards) return;
		
		for(ShardTbl shardTbl : shards) {
			if(shardTbl.getShardName().trim().equals(shard.getShardName().trim())) {
				throw new BadRequestException("Duplicated shard name under same cluster.");
			}
			if(shardTbl.getSetinelMonitorName().trim().equals(shard.getSetinelMonitorName().trim())) {
				throw new BadRequestException("Duplicated sentinel monitor name under same cluster.");
			}
		}
	}

	@DalTransaction
	public void handleShardsUpdate(List<ShardTbl> toCreates, List<ShardTbl> toDeletes, ClusterTbl clusterTbl, DcTbl dcTbl,
								   boolean isDRMaster, List<SentinelGroupModel> sentinels) throws DalException {
		if (toDeletes != null && !toDeletes.isEmpty()) {
			logger.info("[handleShardsUpdate] delete shards : {}", toDeletes.size());
			deleteShardBatch(toDeletes, clusterTbl, dcTbl, isDRMaster);
		}

		if (toCreates != null && !toCreates.isEmpty()) {
			logger.info("[handleShardsUpdate] create shards : {}", toCreates.size());
			createShardBatch(toCreates, clusterTbl, dcTbl, isDRMaster, sentinels);
		}
	}

	@DalTransaction
	private void deleteShardBatch(List<ShardTbl> toDeletes, ClusterTbl clusterTbl, DcTbl dcTbl,
								  boolean isDRMaster) throws DalException {
		List<DcClusterShardTbl> relatedDcClusterShards = new LinkedList<DcClusterShardTbl>();
		if (isDRMaster) {
			for (final ShardTbl toDelete : toDeletes) {
				List<DcClusterShardTbl> dcClusterShardTbls =
						findClusterShardsByName(clusterTbl.getClusterName(), toDelete.getShardName());
				if (null != dcClusterShardTbls && !dcClusterShardTbls.isEmpty()) {
					relatedDcClusterShards.addAll(dcClusterShardTbls);
				}
			}
		} else {
			for (final ShardTbl toDelete : toDeletes) {
				DcClusterShardTbl dcClusterShardTbl =
						findDcClusterShardByName(dcTbl.getDcName(), clusterTbl.getClusterName(), toDelete.getShardName());
				if (null != dcClusterShardTbl) {
					relatedDcClusterShards.add(dcClusterShardTbl);
				}
			}
		}
		dcClusterShardDao.deleteDcClusterShardsBatch(relatedDcClusterShards);
		for(ShardTbl toDelete : toDeletes) {
			toDelete.setShardName(generateDeletedName(toDelete.getShardName()));
		}
		handleBatchDelete(toDeletes);
	}

	public List<DcClusterShardTbl> findClusterShardsByName(String clusterName, String shardName) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dcClusterShardTblDao.findClusterShardByName(clusterName, shardName,
																			DcClusterShardTblEntity.READSET_FULL);
			}
		});
	}

	public DcClusterShardTbl findDcClusterShardByName(String dcName, String clusterName, String shardName) {
		return queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl doQuery() throws DalException {
				return dcClusterShardTblDao.findDcClusterShardByName(dcName, clusterName, shardName,
																			DcClusterShardTblEntity.READSET_FULL);
			}
		});
	}

	@DalTransaction
	private void createShardBatch(List<ShardTbl> toCreate, ClusterTbl clusterTbl, DcTbl dcTbl,
								  boolean isDRMaster, List<SentinelGroupModel> sentinels) {
		List<DcClusterTbl> dcClusterTbls = getRelatedDcClusters(clusterTbl, dcTbl, isDRMaster);

		if (dcClusterTbls == null || dcClusterTbls.isEmpty()) {
			throw new BadRequestException(String.format("[handleShardsUpdate] cluster: %s, dc:%s does not exist," +
					" should create", clusterTbl.getClusterName(), dcTbl.getDcName()));
		}

		List<DcClusterShardTbl> dcClusterShardTbls = new ArrayList<>();
		toCreate.forEach(shard -> {
			ShardTbl proto = shardTblDao.createLocal();
			proto.setClusterId(clusterTbl.getId()).setShardName(shard.getShardName())
					.setSetinelMonitorName(shard.getSetinelMonitorName());
			insertShard(proto);
			ShardTbl newShard = findShardByClusterAndShard(clusterTbl.getClusterName(), shard.getShardName());

			dcClusterTbls.forEach(dcClusterTbl -> {
				DcClusterShardTbl dcClusterShardTbl = dcClusterShardTblDao.createLocal();
				dcClusterShardTbl.setShardId(newShard.getId()).setDcClusterId(dcClusterTbl.getDcClusterId());

				if (sentinels != null && !sentinels.isEmpty()) {
					dcClusterShardTbl.setSetinelId(sentinels.get(random.nextInt(sentinels.size())).getSentinelGroupId());
				}

				dcClusterShardTbls.add(dcClusterShardTbl);
			});
		});

		if (!dcClusterShardTbls.isEmpty()) {
			dcClusterShardDao.insertDcClusterShardsBatch(dcClusterShardTbls);
		}
	}

	private List<DcClusterTbl> getRelatedDcClusters(ClusterTbl clusterTbl, DcTbl dcTbl, boolean isDRMaster) {
		List<DcClusterTbl> result = new ArrayList<>();
		if (isDRMaster) {
			List<DcClusterTbl> dcClusterTbls = queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
				@Override
				public List<DcClusterTbl> doQuery() throws DalException {
					return dcClusterTblDao.findAllByClusterAndGroupType(clusterTbl.getId(), isDRMaster, DcClusterTblEntity.READSET_FULL);
				}
			});
			result.addAll(dcClusterTbls);

		} else {
			DcClusterTbl dcClusterTbl = queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
				@Override
				public DcClusterTbl doQuery() throws DalException {
					return dcClusterTblDao.findDcClusterById(dcTbl.getId(), clusterTbl.getId(), DcClusterTblEntity.READSET_FULL);
				}
			});
			result.add(dcClusterTbl);
		}
		return result;
	}

	public void handleBatchDelete(List<ShardTbl> toDeletes) {
		queryHandler.handleBatchDelete(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return shardTblDao.deleteShardsBatch(toDeletes.toArray(new ShardTbl[toDeletes.size()]),
						ShardTblEntity.UPDATESET_FULL);
			}
		}, true);
	}

	public ShardTbl findShardByClusterAndShard(String clusterName, String shardName) {
		return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return shardTblDao.findShard(clusterName, shardName, ShardTblEntity.READSET_FULL);
			}
		});
	}

	public void insertShard(ShardTbl proto) {
		queryHandler.handleInsert(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return shardTblDao.insert(proto);
			}
		});
	}

	@DalTransaction
	public ShardTbl insertShard(String clusterName, ShardTbl shard, Map<Long, SentinelGroupModel> sentinels) throws DalException{

		final ClusterTbl cluster = clusterTblDao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
		shard.setClusterId(cluster.getId());
		shard.setShardName(shard.getShardName().trim());
		shard.setSetinelMonitorName(shard.getSetinelMonitorName().trim());
		insertShard(shard);

		if (ClusterType.isSameClusterType(cluster.getClusterType(), ClusterType.HETERO)) {
			return shard;
		}

		// dc-cluster-shards
		List<DcClusterTbl> dcClusters = queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dcClusterTblDao.findAllByClusterId(cluster.getId(), DcClusterTblEntity.READSET_FULL);
			}
		});

		if(null != dcClusters && !dcClusters.isEmpty()) {
			List<DcClusterShardTbl> dcClusterShards = new LinkedList<DcClusterShardTbl>();
			for(DcClusterTbl dcCluster : dcClusters) {
				DcClusterShardTbl dcClusterShardProto = dcClusterShardTblDao.createLocal();
				dcClusterShardProto.setDcClusterId(dcCluster.getDcClusterId())
						.setShardId(shard.getId());
				if(sentinels != null && null != sentinels.get(dcCluster.getDcId())) {
					dcClusterShardProto.setSetinelId(sentinels.get(dcCluster.getDcId()).getSentinelGroupId());
				}
				dcClusterShards.add(dcClusterShardProto);
			}
			if (!dcClusterShards.isEmpty()) {
				queryHandler.handleBatchInsert(new DalQuery<int[]>() {
					@Override
					public int[] doQuery() throws DalException {
						return dcClusterShardTblDao.insertBatch(dcClusterShards.toArray(new DcClusterShardTbl[dcClusterShards.size()]));
					}
				});
			}

		}
		return shard;
	}
}
