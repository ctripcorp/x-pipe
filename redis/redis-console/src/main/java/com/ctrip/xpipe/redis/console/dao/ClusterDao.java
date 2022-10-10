package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.console.model.ClusterTblEntity.*;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Repository
public class ClusterDao extends AbstractXpipeConsoleDAO{
	private DcTblDao dcTblDao;
	private ClusterTblDao clusterTblDao;
	private DcClusterTblDao dcClusterTblDao;
	private ShardTblDao shardTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	private ReplDirectionTblDao replDirectionTblDao;
	
	@Autowired
	private ShardDao shardDao;
	@Autowired
	private DcClusterDao dcClusterDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcTblDao = ContainerLoader.getDefaultContainer().lookup(DcTblDao.class);
			clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
			replDirectionTblDao = ContainerLoader.getDefaultContainer().lookup(ReplDirectionTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	
	@DalTransaction
	public ClusterTbl createCluster(final ClusterTbl cluster, final List<DcClusterModel> dcClusterModels) throws DalException {
		// check for unique cluster name
		ClusterTbl clusterWithSameName = queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return clusterTblDao.findClusterByClusterName(cluster.getClusterName(), ClusterTblEntity.READSET_FULL);
			}
		});
		if(null != clusterWithSameName) throw new BadRequestException("Duplicated cluster name");

		cluster.setCreateTime(new Date());
		// cluster meta
		queryHandler.handleInsert(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterTblDao.insert(cluster);
			}
		});

	    ClusterTbl newCluster = clusterTblDao.findClusterByClusterName(cluster.getClusterName(), ClusterTblEntity.READSET_FULL);
	    if (!ClusterType.lookup(newCluster.getClusterType()).supportMultiActiveDC()) {
			// related dc-cluster
			DcTbl activeDc = dcTblDao.findByPK(cluster.getActivedcId(), DcTblEntity.READSET_FULL);
			DcClusterTbl protoDcCluster = dcClusterTblDao.createLocal();
			protoDcCluster.setDcId(activeDc.getId()).setClusterId(newCluster.getId());

			if (dcClusterModels != null && !dcClusterModels.isEmpty()) {
				dcClusterModels.forEach(dcClusterModel -> {
					if (activeDc.getDcName().equalsIgnoreCase(dcClusterModel.getDc().getDc_name())) {
						protoDcCluster.setGroupName(dcClusterModel.getDcCluster().getGroupName())
								.setGroupType(dcClusterModel.getDcCluster().getGroupType());

					}
				});
			}

			queryHandler.handleInsert(new DalQuery<Integer>() {
				@Override
				public Integer doQuery() throws DalException {
					return dcClusterTblDao.insert(protoDcCluster);
				}
			});
		}

		return newCluster;
	}
	
	@DalTransaction
	public void updateCluster(ClusterTbl cluster) {
		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterTblDao.updateByPK(cluster, ClusterTblEntity.UPDATESET_FULL);
			}
		});
	}
	
	@DalTransaction
	public void deleteCluster(final ClusterTbl cluster) throws DalException {
		// Related shards & dcClusters
		List<ShardTbl> shards = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return shardTblDao.findAllByClusterId(cluster.getId(), ShardTblEntity.READSET_FULL);
			}
		});
		List<DcClusterTbl> dcClusters = queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dcClusterTblDao.findAllByClusterId(cluster.getId(), DcClusterTblEntity.READSET_FULL);
			}
		});

		List<ReplDirectionTbl> replDirections = queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
			@Override
			public List<ReplDirectionTbl> doQuery() throws DalException {
				return replDirectionTblDao.findReplDirectionsByCluster(cluster.getId(), ReplDirectionTblEntity.READSET_FULL);
			}
		});
		
		if(null != shards && !shards.isEmpty()) {
			shardDao.deleteShardsBatch(shards);
		}
		
		if(null != dcClusters && !dcClusters.isEmpty()) {
			dcClusterDao.deleteDcClustersBatch(dcClusters);
		}

		if (null != replDirections && !replDirections.isEmpty()) {
			queryHandler.handleBatchDelete(new DalQuery<int[]>() {
				@Override
				public int[] doQuery() throws DalException {
					return replDirectionTblDao.deleteBatch(replDirections.toArray(new ReplDirectionTbl[replDirections.size()]),
							ReplDirectionTblEntity.UPDATESET_FULL);
				}
			}, true);
		}
		
		ClusterTbl proto = cluster;
		proto.setClusterName(generateDeletedName(cluster.getClusterName()));
		queryHandler.handleDelete(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterTblDao.deleteCluster(proto, ClusterTblEntity.UPDATESET_FULL);
			}
		}, true);
		
	}

	@DalTransaction
	public int bindDc(final ClusterTbl cluster, final DcTbl dc, DcClusterTbl dcClusterTbl, SentinelGroupModel sentinel) throws DalException {
		
		DcClusterTbl existingDcCluster = queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalException {
				return dcClusterTblDao.findDcClusterById(dc.getId(), cluster.getId(), DcClusterTblEntity.READSET_FULL);
			}
		});
		// not binded
		if(null == existingDcCluster) {
			DcClusterTbl proto = dcClusterTbl;
			proto.setDcId(dc.getId()).setClusterId(cluster.getId());

			queryHandler.handleInsert(new DalQuery<Integer>() {
				@Override
				public Integer doQuery() throws DalException {
					return dcClusterTblDao.insert(proto);
				}
			});

			if(DcGroupType.isNullOrDrMaster(dcClusterTbl.getGroupType())) {

				List<DcClusterTbl> dcClusters = dcClusterTblDao.findAllByClusterId(cluster.getId(), DcClusterTblEntity.READSET_FULL);

				Set<Long> shardIds = new HashSet<>();
				for (DcClusterTbl dcCluster : dcClusters) {
					if(DcGroupType.isNullOrDrMaster(dcCluster.getGroupType())) {
						List<ShardTbl> shardTbls = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
							@Override
							public List<ShardTbl> doQuery() throws DalException {
								return shardTblDao.findAllShardByDcCluster(dcCluster.getDcId(), cluster.getId(), ShardTblEntity.READSET_FULL);
							}
						});
						List<Long> dcShardIds = shardTbls.stream().map(ShardTbl::getId).collect(Collectors.toList());
						shardIds.addAll(dcShardIds);
					}
				}

				DcClusterTbl dcCluster = dcClusterTblDao.findDcClusterByName(dc.getDcName(), cluster.getClusterName(), DcClusterTblEntity.READSET_FULL);

				if (!shardIds.isEmpty()) {
					List<DcClusterShardTbl> dcClusterShards = new LinkedList<DcClusterShardTbl>();
					for (Long shardId : shardIds) {
						DcClusterShardTbl dcClusterShard = dcClusterShardTblDao.createLocal();
						dcClusterShard.setDcClusterId(dcCluster.getDcClusterId())
								.setShardId(shardId);
						if (sentinel != null) {
							dcClusterShard.setSetinelId(sentinel.getSentinelGroupId());
						}
						if (ClusterType.lookup(cluster.getClusterType()).isCrossDc()) {
							List<DcClusterShardTbl> allShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
								@Override
								public List<DcClusterShardTbl> doQuery() throws DalException {
									return dcClusterShardTblDao.findAllByShardId(dcClusterShard.getShardId(), DcClusterShardTblEntity.READSET_FULL);
								}
							});
							if (allShards != null && !allShards.isEmpty())
								dcClusterShard.setSetinelId(allShards.get(0).getSetinelId());
						}
						dcClusterShards.add(dcClusterShard);
					}
					queryHandler.handleBatchInsert(new DalQuery<int[]>() {
						@Override
						public int[] doQuery() throws DalException {
							return dcClusterShardTblDao.insertBatch(dcClusterShards.toArray(new DcClusterShardTbl[dcClusterShards.size()]));
						}
					});

				}
			}
		}
		
		return 0;
	}
	
	@DalTransaction
	public int unbindDc(final ClusterTbl cluster, final DcTbl dc) throws DalException {
		DcClusterTbl dcCluster = queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalException {
				return dcClusterTblDao.findDcClusterById(dc.getId(), cluster.getId(), DcClusterTblEntity.READSET_FULL); 
			}
		});
		
		if(null != dcCluster) {
			dcClusterDao.deleteDcClustersBatch(dcCluster);
		}
		
		return 0;
	}

	@DalTransaction
	public ClusterTbl findClusterAndOrgByName(final String clusterName) {
		ClusterTbl proto =  queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override public ClusterTbl doQuery() throws DalException {
				return clusterTblDao.findClusterAndOrgByClusterName(clusterName, ClusterTblEntity.READSET_FULL_WITH_ORG);
			}
		});
		String clusterOrgName = proto.getClusterOrgName();
		if(clusterOrgName == null || clusterOrgName.trim().isEmpty()) {
			proto.setClusterOrgName(proto.getOrganizationInfo().getOrgName());
		}
		return proto;
	}

	public ClusterTbl findClusterByClusterName(String clusterName) {
		return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return clusterTblDao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
			}

		});
	}

	public List<ClusterTbl> findAllClusters() {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return clusterTblDao.findAllClusters(ClusterTblEntity.READSET_FULL);
			}
		});
	}

	public List<ClusterTbl> findAllClustersWithCreateTime() {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return clusterTblDao.findAllClusters(ClusterTblEntity.READSET_CREATE_TIME);
			}
		});
	}

	public List<ClusterTbl> findAllClusterWithOrgInfo() {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override public List<ClusterTbl> doQuery() throws DalException {
				return clusterTblDao.findAllClustersWithOrgInfo(ClusterTblEntity.READSET_FULL_WITH_ORG);
			}
		});
	}

	public List<ClusterTbl> findClusterWithOrgInfoByClusterType(String type) {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override public List<ClusterTbl> doQuery() throws DalException {
				return clusterTblDao.findClustersWithOrgInfoByClusterType(type, ClusterTblEntity.READSET_FULL_WITH_ORG);
			}
		});
	}

	public List<ClusterTbl> findClustersWithOrgInfoByActiveDcId(final long dcId) {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override public List<ClusterTbl> doQuery() throws DalException {
				return clusterTblDao.findClustersWithOrgInfoByActiveDcId(dcId, ClusterTblEntity.READSET_FULL_WITH_ORG);
			}
		});
	}

	public List<ClusterTbl> findClustersByActiveDcId(final long activeDcId) {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return clusterTblDao.findClustersByActiveDcId(activeDcId, ClusterTblEntity.READSET_FULL);
			}
		});
	}

	public List<ClusterTbl> findAllByDcId(final long dcId) {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return clusterTblDao.findAllByDcId(dcId, ClusterTblEntity.READSET_FULL);
			}
		});
	}

	public List<ClusterTbl> findClustersWithName(List<String> clusterNames) {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override public List<ClusterTbl> doQuery() throws DalException {
				return clusterTblDao.findClustersAndOrgWithClusterNames(clusterNames,
						ClusterTblEntity.READSET_FULL_WITH_ORG);
			}
		});
	}

	public List<ClusterTbl> findMigratingClustersWithEvents() {
		return queryHandler.handleQuery(() -> {
			return clusterTblDao.findMigratingClustersWithEvents(READSET_FULL_WITH_MIGRATION_EVENT);
		});
	}

	public List<ClusterTbl> findMigratingClustersOverview() {
		return queryHandler.handleQuery(() -> {
			return clusterTblDao.findMigratingClustersOverview(READSET_FULL_WITH_MIGRATION_OVERVIEW);
		});
	}

	public List<ClusterTbl> findMigratingClusterNames() {
		return queryHandler.handleQuery(() -> clusterTblDao.findMigratingClusters(READSET_NAME));
	}
}
