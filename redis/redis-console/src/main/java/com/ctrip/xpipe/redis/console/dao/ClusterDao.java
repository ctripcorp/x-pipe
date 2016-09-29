package com.ctrip.xpipe.redis.console.dao;

import java.util.LinkedList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.SetinelTblEntity;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTblDao;
import com.ctrip.xpipe.redis.console.model.ShardTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Repository
public class ClusterDao extends AbstractXpipeConsoleDAO{
	private DcTblDao dcTblDao;
	private ClusterTblDao clusterTblDao;
	private DcClusterTblDao dcClusterTblDao;
	private ShardTblDao shardTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	private SetinelTblDao setinelTblDao;
	
	@Autowired
	private ShardDao shardDao;
	@Autowired
	private DcClusterDao dcClusterDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcTblDao = ContainerLoader.getDefaultContainer().lookup(DcTblDao.class);
			clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
			setinelTblDao = ContainerLoader.getDefaultContainer().lookup(SetinelTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	
	@DalTransaction
	public ClusterTbl createCluster(final ClusterTbl cluster) throws DalException {
		// check for unique cluster name
		ClusterTbl clusterWithSameName = queryHandler.tryGet(new DalQuery<ClusterTbl>() {
			@Override
			public ClusterTbl doQuery() throws DalException {
				return clusterTblDao.findClusterByClusterName(cluster.getClusterName(), ClusterTblEntity.READSET_FULL);
			}
		});
		if(null != clusterWithSameName) throw new BadRequestException("Duplicated cluster name");
		
		// cluster meta
		clusterTblDao.insert(cluster);
	    
	    // related dc-cluster
	    ClusterTbl newCluster = clusterTblDao.findClusterByClusterName(cluster.getClusterName(), ClusterTblEntity.READSET_FULL);
	    DcTbl activeDc = dcTblDao.findByPK(cluster.getActivedcId(), DcTblEntity.READSET_FULL);
	    DcClusterTbl protoDcCluster = dcClusterTblDao.createLocal();
	    protoDcCluster.setDcId(activeDc.getId())
	    		.setClusterId(newCluster.getId())
	    		.setDcClusterPhase(XpipeConsoleConstant.DEFAULT_DC_CLUSTER_PHASE);
	    dcClusterTblDao.insert(protoDcCluster);
		return newCluster;
	}
	
	@DalTransaction
	public int updateCluster(ClusterTbl cluster) throws DalException {
		return clusterTblDao.updateByPK(cluster, ClusterTblEntity.UPDATESET_FULL);
	}
	
	@DalTransaction
	public int deleteCluster(final ClusterTbl cluster) throws DalException {
		// Related shards & dcClusters
		List<ShardTbl> shards = queryHandler.tryGet(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return shardTblDao.findAllByClusterId(cluster.getId(), ShardTblEntity.READSET_FULL);
			}
		});
		List<DcClusterTbl> dcClusters = queryHandler.tryGet(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dcClusterTblDao.findAllByClusterId(cluster.getId(), DcClusterTblEntity.READSET_FULL);
			}
		});
		
		if(null != shards) {
			shardDao.deleteShardsBatch(shards);
		}
		
		if(null != dcClusters) {
			dcClusterDao.deleteDcClustersBatch(dcClusters);
		}
		
		ClusterTbl proto = cluster;
		proto.setClusterName(generateDeletedName(cluster.getClusterName()));
		return clusterTblDao.deleteCluster(proto, ClusterTblEntity.UPDATESET_FULL);
		
	}

	@DalTransaction
	public int bindDc(final ClusterTbl cluster, final DcTbl dc) throws DalException {
		List<SetinelTbl> setinels = queryHandler.tryGet(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return setinelTblDao.findByDcId(dc.getId(), SetinelTblEntity.READSET_FULL);
			}
		});
		List<ShardTbl> shards = queryHandler.tryGet(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return shardTblDao.findAllByClusterId(cluster.getId(), ShardTblEntity.READSET_FULL);
			}
		});
		
		DcClusterTbl proto = dcClusterTblDao.createLocal();
		proto.setDcId(dc.getId())
			.setClusterId(cluster.getId())
			.setDcClusterPhase(XpipeConsoleConstant.DEFAULT_DC_CLUSTER_PHASE);
		dcClusterTblDao.insert(proto);
		DcClusterTbl dcCluster = dcClusterTblDao.findDcClusterByName(dc.getDcName(), cluster.getClusterName(), DcClusterTblEntity.READSET_FULL);
		
		if(null != shards) {
			SetinelTbl setinel = ((null == setinels || 0 == setinels.size())? setinelTblDao.createLocal().setSetinelId(0) : setinels.get(0));
			List<DcClusterShardTbl> dcClusterShards = new LinkedList<DcClusterShardTbl>();
			for(ShardTbl shard : shards) {
				DcClusterShardTbl dcClusterShard = dcClusterShardTblDao.createLocal();
				dcClusterShard.setDcClusterId(dcCluster.getDcClusterId())
					.setShardId(shard.getId())
					.setSetinelId(setinel.getSetinelId())
					.setDcClusterShardPhase(XpipeConsoleConstant.DEFAULT_DC_CLUSTER_PHASE);
				dcClusterShards.add(dcClusterShard);
			}
			dcClusterShardTblDao.insertBatch(dcClusterShards.toArray(new DcClusterShardTbl[dcClusterShards.size()]));
		}
		
		return 0;
	}
	
	@DalTransaction
	public int unbindDc(final ClusterTbl cluster, final DcTbl dc) throws DalException {
		DcClusterTbl dcCluster = queryHandler.tryGet(new DalQuery<DcClusterTbl>() {
			@Override
			public DcClusterTbl doQuery() throws DalException {
				return dcClusterTblDao.findDcClusterById(dc.getId(), cluster.getId(), DcClusterTblEntity.READSET_FULL); 
			}
		});
		
		if(null != dcCluster) {
			dcClusterDao.deleteDcClustersBatch(dcCluster);
		}
		
		return 0;
	}
}
