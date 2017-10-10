package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Repository
public class ShardDao extends AbstractXpipeConsoleDAO{
	private ClusterTblDao clusterTblDao;
	private DcClusterTblDao dcClusterTblDao;
	private ShardTblDao shardTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	
	@Autowired
	private DcClusterShardDao dcClusterShardDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	@DalTransaction
	public ShardTbl createShard(String clusterName, ShardTbl shard, Map<Long, SetinelTbl> sentinels) throws DalException{
		// shard basic
		validateShard(clusterName, shard);
		final ClusterTbl cluster = clusterTblDao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
		shard.setClusterId(cluster.getId());
		shardTblDao.insert(shard);

		// dc-cluster-shards
		List<DcClusterTbl> dcClusters = queryHandler.handleQuery(new DalQuery<List<DcClusterTbl>>() {
			@Override
			public List<DcClusterTbl> doQuery() throws DalException {
				return dcClusterTblDao.findAllByClusterId(cluster.getId(), DcClusterTblEntity.READSET_FULL);
			}
		});
		
		if(null != dcClusters) {
			List<DcClusterShardTbl> dcClusterShards = new LinkedList<DcClusterShardTbl>();
			for(DcClusterTbl dcCluster : dcClusters) {
				DcClusterShardTbl dcClusterShardProto = dcClusterShardTblDao.createLocal();
				dcClusterShardProto.setDcClusterId(dcCluster.getDcClusterId())
					.setShardId(shard.getId());
				if(sentinels != null && null != sentinels.get(dcCluster.getDcId())) {
					dcClusterShardProto.setSetinelId(sentinels.get(dcCluster.getDcId()).getSetinelId());
				}
				dcClusterShards.add(dcClusterShardProto);
			}
			dcClusterShardTblDao.insertBatch(dcClusterShards.toArray(new DcClusterShardTbl[dcClusterShards.size()]));
			
		}
		return shard;
	}
	
	@DalTransaction
	public void deleteShardsBatch(List<ShardTbl> shards) throws DalException {
		if(null == shards) throw new DalException("Null cannot be deleted.");
		
		List<DcClusterShardTbl> relatedDcClusterShards = new LinkedList<DcClusterShardTbl>();
		for(final ShardTbl shard : shards) {
			List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
				@Override
				public List<DcClusterShardTbl> doQuery() throws DalException {
					return dcClusterShardTblDao.findAllByShardId(shard.getId(), DcClusterShardTblEntity.READSET_FULL);
				}
			});
			
			if(null != dcClusterShards) {
				relatedDcClusterShards.addAll(dcClusterShards);
			}
		}
		dcClusterShardDao.deleteDcClusterShardsBatch(relatedDcClusterShards);
		
		for(ShardTbl shard : shards) {
			shard.setShardName(generateDeletedName(shard.getShardName()));
		}
		shardTblDao.deleteShardsBatch(shards.toArray(new ShardTbl[shards.size()]), ShardTblEntity.UPDATESET_FULL);
	}
	
	@DalTransaction
	public int deleteShardsBatch(final ShardTbl shard) throws DalException {
		if(null == shard) throw new DalException("Null cannot be deleted.");
		
		List<DcClusterShardTbl> relatedDcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
			@Override
			public List<DcClusterShardTbl> doQuery() throws DalException {
				return dcClusterShardTblDao.findAllByShardId(shard.getId(), DcClusterShardTblEntity.READSET_FULL);
			}
		});
		if(null != relatedDcClusterShards) {
			dcClusterShardDao.deleteDcClusterShardsBatch(relatedDcClusterShards);
		}
		
		ShardTbl proto = shard;
		proto.setShardName(generateDeletedName(shard.getShardName()));
		return shardTblDao.deleteShard(proto, ShardTblEntity.UPDATESET_FULL);
	}
	
	private void validateShard(final String clusterName, ShardTbl shard) throws DalException {
		// validate shard name
		List<ShardTbl> shards = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>(){
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return shardTblDao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
			}
		});
		if(null == shards) return;
		
		for(ShardTbl shardTbl : shards) {
			if(shardTbl.getShardName().equals(shard.getShardName())) {
				throw new BadRequestException("Duplicated shard name under same cluster.");
			}
			if(shardTbl.getSetinelMonitorName().equals(shard.getSetinelMonitorName())) {
				throw new BadRequestException("Duplicated sentinel monitor name under same cluster.");
			}
		}
	}
	
}
