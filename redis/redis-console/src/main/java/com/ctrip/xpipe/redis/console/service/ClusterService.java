package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;

import java.util.List;

@Service
public class ClusterService {
    private ClusterTblDao clusterTblDao;
    
    @Autowired
    private DcClusterService dcClusterService;
    @Autowired
    private ShardService shardService;

    @PostConstruct
    private void postConstruct() {
        try {
            clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    public ClusterTbl load(String clusterName) {
        try {
            return clusterTblDao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Cluster not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load cluster failed.", e);
        }
    }

    public ClusterTbl load(Long clusterId) {
        try {
            return clusterTblDao.findByPK(clusterId, ClusterTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Cluster not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load cluster failed.", e);
        }
    }

    public List<ClusterTbl> findAllClusters() {
        try {
            return clusterTblDao.findAllClusters(ClusterTblEntity.READSET_FULL);
        } catch (DalNotFoundException e) {
            throw new DataNotFoundException("Cluser not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load all clusters failed.", e);
        }
    }
    
    public List<ClusterTbl> findClustersBatch(int offset,int length) {
    	try {
			return clusterTblDao.findClustersBatch(length , offset, ClusterTblEntity.READSET_FULL);
		} catch (DalNotFoundException e) {
            throw new DataNotFoundException("Cluser not found.", e);
        } catch (DalException e) {
            throw new ServerException("Load all clusters failed.", e);
        }
    }
    
    public Long getAllCount() {
    	try {
			return clusterTblDao.totalCount(ClusterTblEntity.READSET_COUNT).getCount();
		} catch (DalNotFoundException e) {
            throw new DataNotFoundException("Cluser not found.", e);
        } catch (DalException e) {
            throw new ServerException("Count clusters failed.", e);
        }
    }
    
    @Transactional
    public ClusterTbl createCluster(ClusterTbl cluster) {
    	ClusterTbl proto = clusterTblDao.createLocal();
		
		proto.setClusterName(cluster.getClusterName());
		proto.setActivedcId(cluster.getActivedcId());
		proto.setClusterDescription(cluster.getClusterDescription());
		proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
		
		try {
			clusterTblDao.insert(proto);
		} catch (DalException e) {
			throw new BadRequestException("Cluster cannot be created.", e);
		}
		
		/** TODO **/
		/* Notify meta server */
		
		return load(cluster.getClusterName());
    }
    
    @Transactional
    public void updateCluster(String clusterName, ClusterTbl clusterTbl) {
    	ClusterTbl proto = load(clusterName);
    	
    	proto.setClusterDescription(clusterTbl.getClusterDescription());
    	proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
    	
    	/** TODO **/
    	/* Notify meta server */
    	
    	try {
			clusterTblDao.updateByPK(proto, ClusterTblEntity.UPDATESET_FULL);
		} catch (DalException e) {
			throw new BadRequestException("Cluster cannot be updated", e);
		}
    }
    
    /**
     * Delete cluster info together with dc-cluster & shards under this cluster
     * @param clusterName
     */
    @Transactional
    public void deleteCluster(String clusterName) {
    	/** Cluster info **/
    	ClusterTbl proto = load(clusterName);
    	/** Related dc-cluster **/
    	List<DcClusterTbl> dcClusters = dcClusterService.loadAllByClusterName(clusterName);
    	/** Related shards **/
    	List<ShardTbl> shards = shardService.loadAllByClusterName(clusterName);
    	
    	/** delete shards info **/
    	shardService.deleteShardsBatch(shards);
    	/** delete dc-clusters info **/
    	dcClusterService.deleteDcClustersBatch(dcClusters);
    	/** delete cluster info **/
    	proto.setClusterName(DataModifiedTimeGenerator.generateModifiedTime() + "-" + clusterName);
    	proto.setDeleted(true);
    	try {
			clusterTblDao.updateByPK(proto, ClusterTblEntity.UPDATESET_FULL);
		} catch (DalException e) {
			throw new BadRequestException("Cluster cannot be deleted", e);
		}
    	
    	/** TODO **/
    	/** Notify meta server **/
    	
    }
}
