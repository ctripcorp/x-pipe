package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.redis.console.service.model.SourceModelService;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.List;

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
	public List<DcClusterModel> findRelatedDcClusterModels(long clusterId) {
		List<DcClusterTbl> dcClusterTbls = findClusterRelated(clusterId);

		List<DcClusterModel> result = new ArrayList<>();
		dcClusterTbls.forEach(dcClusterTbl -> {
			result.add(new DcClusterModel().setDcCluster(dcClusterTbl));
		});

		return result;
	}

	@Override
	public void validateDcClusters(List<DcClusterModel> dcClusterModels, ClusterTbl clusterTbl) {
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

	@Override
	public List<DcClusterTbl> findAllByClusterAndGroupType(long clusterId, long dcId, boolean isDRMaster) {
		if (isDRMaster) {
			return queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
				@Override
				public List<DcClusterTbl> doQuery() throws DalException {
					return dao.findAllByClusterAndGroupType(clusterId, isDRMaster, DcClusterTblEntity.READSET_FULL);
				}
			});
		} else {
			List<DcClusterTbl> result = new ArrayList<>();
			result.add(find(dcId, clusterId));
			return result;
		}
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
