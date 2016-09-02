package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;

/**
 * @author shyin
 *
 * Aug 20, 2016
 */
@Service
public class ClusterService extends AbstractConsoleService<ClusterTblDao>{
	public static int NO_ACTIVE_DC = 0;
	
	@Autowired
	private DcService dcService;
    @Autowired
    private ClusterDao clusterDao;
    
    public ClusterTbl load(final String clusterName) {
    	return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return dao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
			}
    		
    	});
    }

    public ClusterTbl load(final Long clusterId) {
    	return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return dao.findByPK(clusterId, ClusterTblEntity.READSET_FULL);
			}
    		
    	});
    }

    public List<ClusterTbl> findAllClusters() {
    	return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findAllClusters(ClusterTblEntity.READSET_FULL);
			}
    	});
    }
    
    public List<ClusterTbl> findAllClusterNames() {
    	return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findAllClusters(ClusterTblEntity.READSET_NAME);
			}
    	});
    }
    
    public Long getAllCount() {
    	return queryHandler.handleQuery(new DalQuery<Long>() {
			@Override
			public Long doQuery() throws DalException {
				return dao.totalCount(ClusterTblEntity.READSET_COUNT).getCount();
			}
    	});
    }
    
    public ClusterTbl createCluster(final ClusterTbl cluster) {
    	// ensure active dc assigned
    	if(NO_ACTIVE_DC == cluster.getActivedcId()) {
    		throw new BadRequestException("No active dc assigned.");
    	}
    	ClusterTbl proto = dao.createLocal();
    	proto.setClusterName(cluster.getClusterName());
    	proto.setActivedcId(cluster.getActivedcId());
    	proto.setClusterDescription(cluster.getClusterDescription());
    	proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
    	
    	final ClusterTbl queryProto = proto;
    	ClusterTbl result =  queryHandler.handleQuery(new DalQuery<ClusterTbl>(){
			@Override
			public ClusterTbl doQuery() throws DalException {
				return clusterDao.createCluster(queryProto);
			}
    	});
    	
    	// TODO
    	// Notify metaserver
    	
    	return result;
    }
    
    public void updateCluster(final String clusterName, final ClusterTbl clusterTbl) {
    	ClusterTbl proto = load(clusterName);
    	if(null == proto) throw new BadRequestException("Cannot find cluster");
    	
		if(proto.getId() != clusterTbl.getId()) {
			throw new BadRequestException("Cluster not match.");
		}
		proto.setClusterDescription(clusterTbl.getClusterDescription());
		proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
		
		final ClusterTbl queryProto = proto;
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.updateCluster(queryProto);
			}
    	});

    	// TODO 
    	/* Notify meta server */
    	
    }
    
    public void deleteCluster(final String clusterName) {
    	ClusterTbl proto = load(clusterName);
    	if(null == proto) throw new BadRequestException("Cannot find cluster");
    	proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
    	
    	final ClusterTbl queryProto = proto;
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.deleteCluster(queryProto);
			}
    	});
    	
    	// TODO
    	/** Notify meta server **/
    	
    }
    
    public void bindDc(final String clusterName, final String dcName) {
    	final ClusterTbl cluster = load(clusterName);
    	final DcTbl dc = dcService.load(dcName);
    	if(null == dc || null == cluster) throw new BadRequestException("Cannot bind dc due to unknown dc or cluster");
    	
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.bindDc(cluster, dc);
			}
    	});
    	
    	// TODO 
    	/** Notify meta server **/
    }
    
    public void unbindDc(final String clusterName, final String dcName) {
    	final ClusterTbl cluster = load(clusterName);
    	final DcTbl dc = dcService.load(dcName);
    	if(null == dc || null == cluster) throw new BadRequestException("Cannot unbind dc due to unknown dc or cluster");
    	
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.unbindDc(cluster, dc);
			}
    	});
    	
    	// TODO
    	/** Notify meta server **/
    	
    }
    
}
