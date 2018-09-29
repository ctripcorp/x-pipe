package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

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
	public DcClusterTbl addDcCluster(String dcName, String clusterName) {
		DcTbl dcInfo = dcService.find(dcName);
    	ClusterTbl clusterInfo = clusterService.find(clusterName);
    	if(null == dcInfo || null == clusterInfo) throw new BadRequestException("Cannot add dc-cluster to an unknown dc or cluster");
    	
    	DcClusterTbl proto = new DcClusterTbl();
    	proto.setDcId(dcInfo.getId());
    	proto.setClusterId(clusterInfo.getId());
    	proto.setDcClusterPhase(1);
    	
    	try {
			dao.insert(proto);
		} catch (DalException e) {
			throw new ServerException("Cannot create dc-cluster.");
		}
 	
    	return find(dcName, clusterName);
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

}
