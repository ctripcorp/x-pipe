package com.ctrip.xpipe.redis.console.service.impl;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;

@Service
public class ClusterServiceImpl extends AbstractConsoleService<ClusterTblDao> implements ClusterService {

	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterDao clusterDao;
	@Autowired
	private ClusterMetaModifiedNotifier notifier;
	@Autowired
	private ShardService shardService;
	
	@Override
	public ClusterTbl find(final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return dao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
			}
    		
    	});
	}

	@Override
	public ClusterTbl find(final long clusterId) {
		return queryHandler.handleQuery(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return dao.findByPK(clusterId, ClusterTblEntity.READSET_FULL);
			}
    		
    	});
	}

	@Override
	public List<ClusterTbl> findAllClusters() {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findAllClusters(ClusterTblEntity.READSET_FULL);
			}
    	});
	}
	
	@Override
	public List<ClusterTbl> findClustersByActiveDcId(final long activeDcId) {
		return queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findClustersByActiveDcId(activeDcId, ClusterTblEntity.READSET_FULL);
			}
		});
	}


	@Override
	public List<String> findAllClusterNames() {

		List<ClusterTbl> clusterTbls = queryHandler.handleQuery(new DalQuery<List<ClusterTbl>>() {
			@Override
			public List<ClusterTbl> doQuery() throws DalException {
				return dao.findAllClusters(ClusterTblEntity.READSET_NAME);
			}
		});

		List<String> clusterNames = new LinkedList<>();

		clusterTbls.forEach( clusterTbl -> clusterNames.add(clusterTbl.getClusterName()));

		return clusterNames;
	}

	@Override
	public Long getAllCount() {
		return queryHandler.handleQuery(new DalQuery<Long>() {
			@Override
			public Long doQuery() throws DalException {
				return dao.totalCount(ClusterTblEntity.READSET_COUNT).getCount();
			}
    	});
	}

	@Override
	@DalTransaction
	public ClusterTbl createCluster(ClusterModel clusterModel) {
		ClusterTbl cluster = clusterModel.getClusterTbl();
    	List<DcTbl> slaveDcs = clusterModel.getSlaveDcs();
    	List<ShardModel> shards = clusterModel.getShards();
    	
    	// ensure active dc assigned
    	if(XPipeConsoleConstant.NO_ACTIVE_DC_TAG == cluster.getActivedcId()) {
    		throw new BadRequestException("No active dc assigned.");
    	}
    	ClusterTbl proto = dao.createLocal();
    	proto.setClusterName(cluster.getClusterName());
    	proto.setActivedcId(cluster.getActivedcId());
    	proto.setClusterDescription(cluster.getClusterDescription());
    	proto.setStatus(ClusterStatus.Normal.toString());
    	proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
    	
    	final ClusterTbl queryProto = proto;
    	ClusterTbl result =  queryHandler.handleQuery(new DalQuery<ClusterTbl>(){
			@Override
			public ClusterTbl doQuery() throws DalException {
				return clusterDao.createCluster(queryProto);
			}
    	});
    	
    	for(DcTbl dc : slaveDcs) {
    		bindDc(cluster.getClusterName(), dc.getDcName());
    	}

    	if(shards != null){
			for (ShardModel shard : shards) {
				shardService.createShard(cluster.getClusterName(), shard.getShardTbl(), shard.getSentinels());
			}
		}
    	
    	return result;
	}

	@Override
	public void updateCluster(String clusterName, ClusterTbl cluster) {
		ClusterTbl proto = find(clusterName);
    	if(null == proto) throw new BadRequestException("Cannot find cluster");
    	
		if(proto.getId() != cluster.getId()) {
			throw new BadRequestException("Cluster not match.");
		}
		proto.setClusterDescription(cluster.getClusterDescription());
		proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
		
		final ClusterTbl queryProto = proto;
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.updateCluster(queryProto);
			}
    	});
	}

	@Override
	public void updateActivedcId(long id, long activeDcId) {

		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(id);
		clusterTbl.setActivedcId(activeDcId);

		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateActivedcId(clusterTbl, ClusterTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void updateStatusById(long id, ClusterStatus clusterStatus) {

		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(id);
		clusterTbl.setStatus(clusterStatus.toString());

		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return dao.updateStatusById(clusterTbl, ClusterTblEntity.UPDATESET_FULL);
			}
		});
	}

	@Override
	public void deleteCluster(String clusterName) {
		ClusterTbl proto = find(clusterName);
    	if(null == proto) throw new BadRequestException("Cannot find cluster");
    	proto.setClusterLastModifiedTime(DataModifiedTimeGenerator.generateModifiedTime());
    	List<DcTbl> relatedDcs = dcService.findClusterRelatedDc(clusterName);
    	
    	final ClusterTbl queryProto = proto;
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.deleteCluster(queryProto);
			}
    	});
    	
    	/** Notify meta server **/
    	notifier.notifyClusterDelete(clusterName, relatedDcs);
	}

	@Override
	public void bindDc(String clusterName, String dcName) {
		final ClusterTbl cluster = find(clusterName);
    	final DcTbl dc = dcService.find(dcName);
    	if(null == dc || null == cluster) throw new BadRequestException("Cannot bind dc due to unknown dc or cluster");
    	
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.bindDc(cluster, dc);
			}
    	});
	}

	@Override
	public void unbindDc(String clusterName, String dcName) {
		final ClusterTbl cluster = find(clusterName);
    	final DcTbl dc = dcService.find(dcName);
    	if(null == dc || null == cluster) throw new BadRequestException("Cannot unbind dc due to unknown dc or cluster");
    	
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return clusterDao.unbindDc(cluster, dc);
			}
    	});
    	
    	/** Notify meta server **/
    	notifier.notifyClusterDelete(clusterName, Arrays.asList(new DcTbl[]{dc}));
    	
	}

	@Override
	public void update(final ClusterTbl cluster) {
		queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				dao.updateByPK(cluster, ClusterTblEntity.UPDATESET_FULL);
				return 0;
			}
    	});
	}

}
