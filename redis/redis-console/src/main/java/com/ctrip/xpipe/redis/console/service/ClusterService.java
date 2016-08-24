package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import java.util.List;

/**
 * @author shyin
 *
 * Aug 20, 2016
 */
@Service
public class ClusterService extends AbstractConsoleService<ClusterTblDao>{
 
    @Autowired
    private DcClusterService dcClusterService;
    @Autowired
    private ShardService shardService;

    public ClusterTbl load(final String clusterName) {
    	return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalNotFoundException, DalException {
				return dao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
			}
    		
    	});
    }

    public ClusterTbl load(final Long clusterId) {
    	return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalNotFoundException, DalException {
				return dao.findByPK(clusterId, ClusterTblEntity.READSET_FULL);
			}
    		
    	});
    }

    public List<ClusterTbl> findAllClusters() {
    	return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalNotFoundException, DalException {
				return dao.findAllClusters(ClusterTblEntity.READSET_FULL);
			}
    	});
    }
    
    public List<ClusterTbl> findAllClusterNames() {
    	return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalNotFoundException, DalException {
				return dao.findAllClusters(ClusterTblEntity.READSET_NAME);
			}
    	});
    }
    
    public Long getAllCount() {
    	return queryHandler.handleQuery(new DalQuery<Long>() {
			@Override
			public Long doQuery() throws DalNotFoundException, DalException {
				return dao.totalCount(ClusterTblEntity.READSET_COUNT).getCount();
			}
    	});
    }
    
    @DalTransaction
    public ClusterTbl createCluster(final ClusterTbl cluster) {
    	ClusterTbl proto = dao.createLocal();
		
		proto.setClusterName(cluster.getClusterName());
		proto.setActivedcId(cluster.getActivedcId());
		proto.setClusterDescription(cluster.getClusterDescription());
		proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
		
		try {
			dao.insert(proto);
		} catch (DalException e) {
			throw new BadRequestException("Cluster cannot be created.", e);
		}
		
		// TODO
		/* Notify meta server */

		return load(cluster.getClusterName());
    }
    
    public void updateCluster(String clusterName, ClusterTbl clusterTbl) {
    	ClusterTbl proto = load(clusterName);
    	
    	proto.setClusterDescription(clusterTbl.getClusterDescription());
    	proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
    	
    	// TODO 
    	/* Notify meta server */
    	
    	try {
			dao.updateByPK(proto, ClusterTblEntity.UPDATESET_FULL);
		} catch (DalException e) {
			throw new BadRequestException("Cluster cannot be updated", e);
		}
    }
    
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
			dao.updateByPK(proto, ClusterTblEntity.UPDATESET_FULL);
		} catch (DalException e) {
			throw new BadRequestException("Cluster cannot be deleted", e);
		}
    	
    	// TODO
    	/** Notify meta server **/
    	
    }
}
