package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.dao.DcClusterDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.redis.console.service.model.SourceModelService;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.*;

@Service
public class DcClusterServiceImpl extends AbstractConsoleService<DcClusterTblDao> implements DcClusterService {
	
	@Autowired
	private DcService dcService;

	@Autowired
	private ClusterService clusterService;

	@Autowired
	private ShardModelService shardModelService;

	@Autowired
	private SourceModelService sourceModelService;

	@Autowired
	private SentinelBalanceService sentinelBalanceService;

	@Autowired
	private ShardService shardService;

	@Autowired
	private ConsoleConfig consoleConfig;

	@Autowired
	private DcClusterShardService dcClusterShardService;

	@Autowired
	private DcClusterDao dcClusterDao;

	private Comparator<DcClusterModel> dcClusterModelComparator = new Comparator<DcClusterModel>() {
		@Override
		public int compare(DcClusterModel o1, DcClusterModel o2) {
			if (o1 != null && o2 != null && o1.getDcCluster() != null && o2.getDcCluster() != null
					&& ObjectUtils.equals(o1.getDcCluster().getDcId(), o2.getDcCluster().getDcId())
					&& ObjectUtils.equals(o1.getDcCluster().getClusterId(), o2.getDcCluster().getClusterId())
					&& ObjectUtils.equals(o1.getDcCluster().getGroupName(), o2.getDcCluster().getGroupName())
					&& ObjectUtils.equals(o1.getDcCluster().isGroupType(), o2.getDcCluster().isGroupType())) {
				return 0;
			}
			return -1;
		}
	};
	
	@Override
	public DcClusterTbl find(final long dcId, final long clusterId) {
		return queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalException {
	    		return dao.findDcClusterById(dcId, clusterId, DcClusterTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public DcClusterTbl find(final String dcName, final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalException {
				return dao.findDcClusterByName(dcName, clusterName, DcClusterTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public DcClusterCreateInfo findDcClusterCreateInfo(final String dcName, final String clusterName) {
		DcClusterTbl dcClusterTbl = find(dcName, clusterName);

		return new DcClusterCreateInfo().setClusterName(clusterService.find(dcClusterTbl.getClusterId()).getClusterName())
				.setDcName(dcService.find(dcClusterTbl.getDcId()).getDcName())
				.setRedisCheckRule(dcClusterTbl.getActiveRedisCheckRules());
	}


	@Override
	public void updateDcCluster(DcClusterCreateInfo dcClusterCreateInfo) {
		DcClusterTbl dcClusterTbl = find(dcClusterCreateInfo.getDcName(), dcClusterCreateInfo.getClusterName());
		if (dcClusterTbl == null)
			throw new BadRequestException(String.format("Can not update unexist dcCluster %s:%s",
					dcClusterCreateInfo.getDcName(), dcClusterCreateInfo.getClusterName()));

		dcClusterTbl.setActiveRedisCheckRules(dcClusterCreateInfo.getRedisCheckRule());

		queryHandler.handleUpdate(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateByPK(dcClusterTbl, DcClusterTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void updateDcClustersByDcClusterModels(List<DcClusterModel> dcClusterModels, ClusterTbl clusterTbl) {
		if (clusterTbl == null) {
			throw new BadRequestException("[updateDcClustersByDcClusterModels] cluster can not be null!");
		}
		validateDcClusters(dcClusterModels, clusterTbl);
		List<DcClusterModel> originDcClusters = findRelatedDcClusterModels(clusterTbl.getId());

		updateDcClustersByDcClusterModels(originDcClusters, dcClusterModels, clusterTbl);
	}

	public List<DcClusterModel> findRelatedDcClusterModels(long clusterId) {
		List<DcClusterTbl> dcClusterTbls = findClusterRelated(clusterId);

		List<DcClusterModel> result = new ArrayList<>();
		dcClusterTbls.forEach(dcClusterTbl -> {
			result.add(new DcClusterModel().setDcCluster(dcClusterTbl));
		});

		return result;
	}

	private void updateDcClustersByDcClusterModels(List<DcClusterModel> originDcClusters,
												   List<DcClusterModel> targetDcClusters, ClusterTbl clusterTbl){
		List<DcClusterModel> toCreates = (List<DcClusterModel>) setOperator.difference(DcClusterModel.class,
														targetDcClusters, originDcClusters, dcClusterModelComparator);
		List<DcClusterModel> toDeletes = (List<DcClusterModel>) setOperator.difference(DcClusterModel.class,
														originDcClusters, targetDcClusters, dcClusterModelComparator);
		List<DcClusterModel> left = (List<DcClusterModel>) setOperator.intersection(DcClusterModel.class,
														originDcClusters, targetDcClusters, dcClusterModelComparator);

		try {
			handleUpdateDcClusters(toCreates, toDeletes, left, clusterTbl);
		} catch (Exception e) {
			throw new ServerException(e.getMessage());
		}
	}

	private void handleUpdateDcClusters(List<DcClusterModel> toCreates, List<DcClusterModel> toDeletes,
										List<DcClusterModel> toUpdates, ClusterTbl clusterTbl) throws DalException {
		if (toDeletes != null && !toDeletes.isEmpty()) {
			logger.info("[updateDcClustersByDcClusterModels] delete dc cluster {}, {}", toDeletes.size(), toDeletes);
			deleteDcClusterBatch(toDeletes, clusterTbl);
		}

		if (toCreates != null && !toCreates.isEmpty()) {
			logger.info("[updateDcClustersByDcClusterModels] create dc cluster {}, {}", toCreates.size(), toCreates);
			createDcClusterBatch(toCreates, clusterTbl);
		}

		if (toUpdates != null && !toUpdates.isEmpty()) {
			logger.info("[updateDcClustersByDcClusterModels] update dc cluster {}, {}", toUpdates.size(), toUpdates);
			updateDcClusterBatch(toUpdates, clusterTbl);
		}
	}

	private void createDcClusterBatch(List<DcClusterModel> toCreates, ClusterTbl clusterTbl) {
		toCreates.forEach((toCreate)->{
			addDcClusterByDcClusterModel(toCreate, clusterTbl);
		});
	}

	private void deleteDcClusterBatch(List<DcClusterModel> toDeletes, ClusterTbl clusterTbl) throws DalException {
		for (DcClusterModel toDelete : toDeletes){
			if (toDelete.getDcCluster().getDcId() == clusterTbl.getActivedcId()) {
				throw new BadRequestException("can not delete active dc");
			}
			dcClusterDao.deleteDcClusterBatchByDcClusterModel(toDelete, clusterTbl);
		}
	}

	private void updateDcClusterBatch(List<DcClusterModel> toUpdates, ClusterTbl clusterTbl) throws DalException {
		for (DcClusterModel toUpdate : toUpdates){
			shardService.updateShardsByDcClusterModel(toUpdate, clusterTbl);
		}
	}

	@DalTransaction
	private void addDcClusterByDcClusterModel(DcClusterModel dcClusterModel, ClusterTbl clusterTbl) {
		DcClusterTbl exist = find(dcClusterModel.getDcCluster().getDcId(), dcClusterModel.getDcCluster().getClusterId());
		if (exist != null) {
			throw new BadRequestException(String.format("[updateDcClustersByDcClusterModels]DcCluster dc:%s cluster:%s exist",
					dcClusterModel.getDcCluster().getDcId(), dcClusterModel.getDcCluster().getClusterId()));
		}

		DcClusterTbl proto = dao.createLocal();
		proto.setDcId(dcClusterModel.getDcCluster().getDcId())
				.setClusterId(dcClusterModel.getDcCluster().getClusterId())
				.setDcClusterPhase(1)
				.setGroupType(dcClusterModel.getDcCluster().isGroupType())
				.setGroupName(dcClusterModel.getDcCluster().getGroupName());
		queryHandler.handleInsert(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.insert(proto);
			}
		});

		if (dcClusterModel.getShards() == null || dcClusterModel.getShards().isEmpty()) {
			return;
		}

		List<DcClusterTbl> relatedDcClusters =getRelatedDcClusters(clusterTbl, dcClusterModel);
		if (relatedDcClusters == null || relatedDcClusters.isEmpty()) {
			throw new BadRequestException(String.format("insert dc %s cluster %s fail",
					dcClusterModel.getDc().getDc_name(), clusterTbl.getClusterName()));
		}

		Map<Long, String> dcNameMap = dcService.dcNameMap();
		List<DcClusterShardTbl> dcClusterShardTbls = new LinkedList<>();
		for (ShardModel shard : dcClusterModel.getShards()) {
			ShardTbl newShard = shardService.findOrCreateShardIfNotExist(clusterTbl.getClusterName(), shard.getShardTbl(), null, null);

			List<DcClusterShardTbl> relatedDcClusterShards = getRelatedDcClusterShards(newShard, relatedDcClusters, clusterTbl, dcNameMap);
			dcClusterShardTbls.addAll(relatedDcClusterShards);
		}

		dcClusterShardService.insertBatch(dcClusterShardTbls);
	}

	private List<DcClusterTbl>  getRelatedDcClusters(ClusterTbl clusterTbl, DcClusterModel dcClusterModel) {
		List<DcClusterTbl> result = new ArrayList<>();
		if (dcClusterModel.getDcCluster().isGroupType()) {
			result.addAll(findAllByClusterAndGroupType(clusterTbl.getId(), dcClusterModel.getDcCluster().isGroupType()));
		} else {
			result.add(find(dcClusterModel.getDc().getDc_name(), clusterTbl.getClusterName()));
		}
		return result;
	}

	private List<DcClusterShardTbl> getRelatedDcClusterShards(ShardTbl shard, List<DcClusterTbl> relatedDcClusters,
															  ClusterTbl clusterTbl, Map<Long, String> dcNameMap) {
		List<DcClusterShardTbl> result = new ArrayList<>();
		for (DcClusterTbl dcClusterTbl : relatedDcClusters){
			DcClusterShardTbl exits = dcClusterShardService.find(dcClusterTbl.getDcClusterId(), shard.getId());
			if (exits != null) continue;

			DcClusterShardTbl dcClusterShardTbl = new DcClusterShardTbl();
			dcClusterShardTbl.setDcClusterId(dcClusterTbl.getDcClusterId()).setShardId(shard.getId());
			ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());

			if (consoleConfig.supportSentinelHealthCheck(clusterType, clusterTbl.getClusterName())) {
				SentinelGroupModel sentinelGroupModel =
						sentinelBalanceService.selectSentinel(dcNameMap.get(dcClusterTbl.getDcId()), clusterType);
				dcClusterShardTbl.setSetinelId(sentinelGroupModel == null ? 0L : sentinelGroupModel.getSentinelGroupId());
			}
			if (clusterType.isCrossDc()) {
				List<DcClusterShardTbl> allShards = dcClusterShardService.findAllDcClusterTblsByShard(shard.getId());
				if (allShards != null && !allShards.isEmpty())
					dcClusterShardTbl.setSetinelId(allShards.get(0).getSetinelId());
			}
			result.add(dcClusterShardTbl);
		}
		return result;
	}

	private void validateDcClusters(List<DcClusterModel> dcClusterModels, ClusterTbl clusterTbl) {
		dcClusterModels.forEach(dcClusterModel -> {
			if (clusterTbl.getId() != dcClusterModel.getDcCluster().getClusterId()) {
				throw new BadRequestException(String.format("dc cluster:{} should belong to cluster:{}, but belong to cluster:{}",
						dcClusterModel.getDcCluster(), dcClusterModel.getDcCluster().getClusterId(), clusterTbl.getId()));
			}

			if (clusterTbl.getActivedcId() == dcClusterModel.getDcCluster().getDcId()
					&& !dcClusterModel.getDcCluster().isGroupType()) {
				throw new BadRequestException(String.format("active dc %d of cluster %s must be DRMaster",
						clusterTbl.getActivedcId(), clusterTbl.getClusterName()));
			}
		});
	}

	public List<DcClusterTbl> findAllByClusterAndGroupType(long clusterId, boolean isDRMaster) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dao.findAllByClusterAndGroupType(clusterId, isDRMaster, DcClusterTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public DcClusterTbl addDcCluster(String dcName, String clusterName, String redisRule) {
		DcTbl dcInfo = dcService.find(dcName);
		ClusterTbl clusterInfo = clusterService.find(clusterName);
		if(null == dcInfo || null == clusterInfo) throw new BadRequestException("Cannot add dc-cluster to an unknown dc or cluster");

		DcClusterTbl dcClusterTbl = find(dcName, clusterName);
		if(dcClusterTbl != null)
			throw new BadRequestException(String.format("DcCluster dc:%s cluster:%s exist", dcName, clusterName));

		DcClusterTbl proto = new DcClusterTbl();
		proto.setDcId(dcInfo.getId());
		proto.setClusterId(clusterInfo.getId());
		proto.setDcClusterPhase(1);
		proto.setActiveRedisCheckRules(redisRule);

		try {
			dao.insert(proto);
		} catch (DalException e) {
			throw new ServerException("Cannot create dc-cluster.");
		}

		return find(dcName, clusterName);
	}


	@Override
	public DcClusterTbl addDcCluster(String dcName, String clusterName) {
		return addDcCluster(dcName, clusterName, null);
	}


	@Override
	public List<DcClusterTbl> findAllDcClusters() {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dao.findAll(DcClusterTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<DcClusterTbl> findByClusterIds(final List<Long> clusterIds) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dao.findByClusterIds(clusterIds, DcClusterTblEntity.READSET_FULL_WITH_DC);
			}
		});
	}

	@Override
	public List<DcClusterTbl>  findAllByDcId(final long dcId){
		return queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dao.findAllByDcId(dcId, DcClusterTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<DcClusterTbl> findClusterRelated(long clusterId) {
		return queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dao.findAllByClusterId(clusterId, DcClusterTblEntity.READSET_FULL);
			}
		});
	}

	@Override
	public List<DcClusterCreateInfo> findClusterRelated(String clusterName) {
		ClusterTbl clusterTbl = clusterService.find(clusterName);
		if(clusterTbl == null)
			throw new BadRequestException(String.format("cluster %s is unexist", clusterName));

		List<DcClusterTbl> dcClusterTbls = queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dao.findAllByClusterId(clusterTbl.getId(), DcClusterTblEntity.READSET_FULL);
			}
		});

		return Lists.newArrayList(Lists.transform(dcClusterTbls, new Function<DcClusterTbl, DcClusterCreateInfo>() {
			@Override
			public DcClusterCreateInfo apply(DcClusterTbl dcClusterTbl) {

				return new DcClusterCreateInfo().setClusterName(clusterService.find(dcClusterTbl.getClusterId()).getClusterName())
						.setDcName(dcService.find(dcClusterTbl.getDcId()).getDcName())
						.setRedisCheckRule(dcClusterTbl.getActiveRedisCheckRules());
			}
		}));
	}

	@Override
	public DcClusterModel findDcClusterModelByClusterAndDc(String clusterName, String dcName) {
		DcClusterModel result = new DcClusterModel();
		DcModel dcModel = dcService.findDcModelByDcName(dcName);
		if (dcModel == null) {
			throw new BadRequestException(String.format("dc %s does not exist", dcName));
		}
		result.setDc(dcModel);

		DcClusterTbl dcClusterTbl = find(dcName, clusterName);
		if (dcClusterTbl == null) {
			throw new BadRequestException(String.format("cluster %s does not have dc %s", clusterName, dcName));
		}
		result.setDcCluster(dcClusterTbl);

		result.setShards(shardModelService.getAllShardModel(dcName, clusterName));
		if (!dcClusterTbl.isGroupType()) {
			result.setSources(sourceModelService.getAllSourceModels(dcName, clusterName));
		}
		return result;
	}

	@Override
	public List<DcClusterModel> findDcClusterModelsByCluster(String clusterName) {
		ClusterTbl clusterTbl = clusterService.find(clusterName);
		if(clusterTbl == null)
			throw new BadRequestException(String.format("cluster %s does not exist", clusterName));

		List<DcClusterTbl> dcClusterTbls = queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dao.findAllByClusterId(clusterTbl.getId(), DcClusterTblEntity.READSET_FULL);
			}
		});

		List<DcClusterModel> result = new ArrayList<>();
		dcClusterTbls.forEach(dcClusterTbl -> {
			DcClusterModel dcClusterModel = new DcClusterModel().setDcCluster(dcClusterTbl);
			DcModel dcModel = dcService.findDcModelByDcId(dcClusterTbl.getDcId());
			if (dcModel == null) {
				throw new BadRequestException(String.format("dc %s does not exist", dcClusterTbl.getDcId()));
			}
			dcClusterModel.setDc(dcModel);

			dcClusterModel.setShards(shardModelService.getAllShardModel(dcModel.getDc_name(), clusterName));
			if (!dcClusterTbl.isGroupType()) {
				dcClusterModel.setSources(sourceModelService.getAllSourceModels(dcModel.getDc_name(), clusterName));
			}
			result.add(dcClusterModel);
		});


		return result;
	}
}
