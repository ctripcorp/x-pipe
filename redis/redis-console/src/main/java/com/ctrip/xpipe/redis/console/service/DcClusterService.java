package com.ctrip.xpipe.redis.console.service;


import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

/**
 * @author shyin
 *
 * Aug 20, 2016
 */
@Service
public class DcClusterService extends AbstractConsoleService<DcClusterTblDao>{
    @Autowired
    private DcService dcService;
    @Autowired
    private ClusterService clusterService;

    public DcClusterTbl load(final long dcId, final long clusterId) {
    	return queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalException {
	    		return dao.findDcClusterById(dcId, clusterId, DcClusterTblEntity.READSET_FULL);
			}
    	});
    }

    public DcClusterTbl load(final String dcName, final String clusterName){
    	return queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalException {
				return dao.findDcClusterByName(dcName, clusterName, DcClusterTblEntity.READSET_FULL);
			}
    	});
    }
    
    public DcClusterTbl addDcCluster(String dcName, String clusterName) {
    	DcTbl dcInfo = dcService.load(dcName);
    	ClusterTbl clusterInfo = clusterService.load(clusterName);
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
 	
    	return load(dcName, clusterName);
    }
    
}
