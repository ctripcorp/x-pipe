package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.ShardService;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Repository
public class DcClusterDao extends AbstractXpipeConsoleDAO{
	private DcClusterTblDao dcClusterTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	private ShardTblDao shardTblDao;
	private ApplierTblDao applierTblDao;
	
	@Autowired
	private DcClusterShardDao dcClusterShardDao;

	@Autowired
	private ShardService shardService;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
			applierTblDao = ContainerLoader.getDefaultContainer().lookup(ApplierTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	@DalTransaction
	public void deleteDcClustersBatch(List<DcClusterTbl> dcClusters) throws DalException {
		if(null == dcClusters || dcClusters.isEmpty()) {
			logger.warn("[deleteDcClustersBatch] Empty dcClusters list: {}", dcClusters);
			return;
		}
		
		List<DcClusterShardTbl> dcClusterShards = new LinkedList<DcClusterShardTbl>();
		for(final DcClusterTbl dcCluster : dcClusters) {
			List<DcClusterShardTbl> relatedDcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>(){
				@Override
				public List<DcClusterShardTbl> doQuery() throws DalException {
					return dcClusterShardTblDao.findAllByDcClusterId(dcCluster.getDcClusterId(), DcClusterShardTblEntity.READSET_FULL);
				}
			});
			
			if(null != relatedDcClusterShards) {
				dcClusterShards.addAll(relatedDcClusterShards);
			}
		}
		dcClusterShardDao.deleteDcClusterShardsBatch(dcClusterShards);

		queryHandler.handleBatchDelete(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return dcClusterTblDao.deleteBatch(dcClusters.toArray(new DcClusterTbl[dcClusters.size()]),
						DcClusterTblEntity.UPDATESET_FULL);
			}
		}, true);

	}

	@DalTransaction
	public void deleteDcClustersBatch(final DcClusterTbl dcCluster) throws DalException  {
		if(null == dcCluster) throw new DalException("Null cannot be deleted.");
		
		List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>(){
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dcClusterShardTblDao.findAllByDcClusterId(dcCluster.getDcClusterId(), DcClusterShardTblEntity.READSET_FULL);
			}
		});
		
		if(null != dcClusterShards) {
			dcClusterShardDao.deleteDcClusterShardsBatch(dcClusterShards);
		}

		queryHandler.handleDelete(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dcClusterTblDao.deleteBatch(dcCluster, DcClusterTblEntity.UPDATESET_FULL);
			}
		}, true);
	}

	@DalTransaction
	public void deleteDcClusterBatchByDcClusterModel(final DcClusterModel dcClusterModel,
													 final ClusterTbl cluster) throws DalException {
		List<ShardTbl> shardTbls = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return shardTblDao.findAllShardByDcCluster(dcClusterModel.getDcCluster().getDcId(),
						dcClusterModel.getDcCluster().getClusterId(), ShardTblEntity.READSET_FULL);
			}
		});

		deleteDcClustersBatch(dcClusterModel.getDcCluster());

		if (!dcClusterModel.getDcCluster().isGroupType()) {
			deleteAppliersAndKeeperWhenMasterDcDeleted(dcClusterModel);
		}

		deleteShardsWhenNoDcClusterShards(shardTbls, cluster);

	}

	private void deleteShardsWhenNoDcClusterShards(List<ShardTbl> shardTbls, ClusterTbl cluster) {
		List<ShardTbl> toDeleteShards = new ArrayList<>();
		shardTbls.forEach(shardTbl -> {
			List<DcClusterShardTbl> existDcClusterShard = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
				@Override
				public List<DcClusterShardTbl> doQuery() throws DalException {
					return dcClusterShardTblDao.findAllByShardId(shardTbl.getId(), DcClusterShardTblEntity.READSET_FULL);
				}
			});

			if (existDcClusterShard == null || existDcClusterShard.isEmpty()) {
				toDeleteShards.add(shardTbl);
			}
		});

		if (!toDeleteShards.isEmpty()) {
			queryHandler.handleBatchDelete(new DalQuery<int[]>() {
				@Override
				public int[] doQuery() throws DalException {
					return shardTblDao.deleteShardsBatch(toDeleteShards.toArray(new ShardTbl[toDeleteShards.size()]),
							ShardTblEntity.UPDATESET_FULL);
				}
			}, true);
		}
		shardService.deleteShardSentinels(shardTbls, cluster);

	}

	private void deleteAppliersAndKeeperWhenMasterDcDeleted(DcClusterModel dcClusterModel) {
		List<ApplierTbl> toDeleteAppliers = queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
			@Override
			public List<ApplierTbl> doQuery() throws DalException {
				return applierTblDao.findAppliersByClusterAndToDc(dcClusterModel.getDcCluster().getDcId(),
						dcClusterModel.getDcCluster().getClusterId(), ApplierTblEntity.READSET_FULL);
			}
		});

		if (toDeleteAppliers != null && !toDeleteAppliers.isEmpty()) {
			queryHandler.handleBatchDelete(new DalQuery<int[]>() {
				@Override
				public int[] doQuery() throws DalException {
					return applierTblDao.deleteBatch(toDeleteAppliers.toArray(new ApplierTbl[toDeleteAppliers.size()]),
							ApplierTblEntity.UPDATESET_FULL);
				}
			}, true);
		}

		//TODO song_yu delete keepers
	}

}
