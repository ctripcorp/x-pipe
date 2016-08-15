package com.ctrip.xpipe.redis.console.service;


import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.ContainerLoader;

import java.util.List;

import javax.annotation.PostConstruct;

@Service
public class DcClusterService {
    private DcClusterTblDao dcClusterTblDao;
    
    @Autowired
    private DcService dcService;
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private MetaserverService metaserverService;

    @PostConstruct
    private void postConstruct() {
        try {
            dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    public DcClusterTbl load(long dcId, long clusterId) {
        try {
            return dcClusterTblDao.findDcClusterById(dcId, clusterId, DcClusterTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Dc-cluster not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load dc-cluster failed.", e);
        }
    }

    public DcClusterTbl load(String dcName, String clusterName){
        try {
            return dcClusterTblDao.findDcClusterByName(dcName, clusterName, DcClusterTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Dc-cluster not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load dc-cluster failed.", e);
        }
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
			dcClusterTblDao.insert(proto);
		} catch (DalException e) {
			throw new ServerException("Cannot create dc-cluster.");
		}
    }
    
    public List<DcClusterTbl> loadAllByClusterName(String clusterName) {
    	/** TODO **/
    	return null;
    }
    
    @Transactional
    public void deleteDcClusters(String dcName, String clusterName) {
    	/** TODO **/
    }
    
    @Transactional
    public void deleteDcClustersBatch(List<DcClusterTbl> dcClusters) {
    	/** TODO **/
    }
    
}
