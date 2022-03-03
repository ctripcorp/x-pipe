package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.LinkedList;
import java.util.List;

@Service
public class DcClusterServiceImpl extends AbstractConsoleService<DcClusterTblDao> implements DcClusterService {
	
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	
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

}
