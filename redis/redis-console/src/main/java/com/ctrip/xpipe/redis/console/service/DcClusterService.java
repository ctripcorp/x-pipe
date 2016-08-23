package com.ctrip.xpipe.redis.console.service;


import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import java.util.List;

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
    @Autowired
    private MetaserverService metaserverService;

    public DcClusterTbl load(final long dcId, final long clusterId) {
    	return queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalNotFoundException, DalException {
	    		return dao.findDcClusterById(dcId, clusterId, DcClusterTblEntity.READSET_FULL);
			}
    	});
    }

    public DcClusterTbl load(final String dcName, final String clusterName){
    	return queryHandler.handleQuery(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalNotFoundException, DalException {
				return dao.findDcClusterByName(dcName, clusterName, DcClusterTblEntity.READSET_FULL);
			}
    	});
    }
    
    @Transactional
    public void addDcCluster(String dcName, String clusterName) {
    	DcTbl dcInfo = dcService.load(dcName);
    	ClusterTbl clusterInfo = clusterService.load(clusterName);
    	List<MetaserverTbl> metaservers = metaserverService.findByDcName(dcInfo.getDcName());
    	
    	DcClusterTbl proto = new DcClusterTbl();
    	proto.setDcId(dcInfo.getId());
    	proto.setClusterId(clusterInfo.getId());
    	for(MetaserverTbl metaserver : metaservers) {
    		if(metaserver.isMetaserverActive()) {
    			proto.setMetaserverId(metaserver.getId());
    			break;
    		}
    	}
    	proto.setDcClusterPhase(1);
    	
    	try {
			dao.insert(proto);
		} catch (DalException e) {
			throw new ServerException("Cannot create dc-cluster.");
		}
    }
    
    public List<DcClusterTbl> loadAllByClusterName(String clusterName) {
    	// TODO
    	return null;
    }
    
    @Transactional
    public void deleteDcClusters(String dcName, String clusterName) {
    	// TODO
    }
    
    @Transactional
    public void deleteDcClustersBatch(List<DcClusterTbl> dcClusters) {
    	// TODO
    }
    
}
