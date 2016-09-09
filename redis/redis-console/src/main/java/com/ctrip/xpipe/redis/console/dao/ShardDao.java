package com.ctrip.xpipe.redis.console.dao;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterTblEntity;
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
public class ShardDao extends AbstractXpipeConsoleDAO{
	private ClusterTblDao clusterTblDao;
	private DcClusterTblDao dcClusterTblDao;
	private ShardTblDao shardTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	private SetinelTblDao setinelTblDao;
	
	@Autowired
	private DcClusterShardDao dcClusterShardDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
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
	public ShardTbl createShard(String clusterName, ShardTbl shard) throws DalException{
		// shard basic
		validateShard(clusterName, shard);
		final ClusterTbl cluster = clusterTblDao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
		shard.setClusterId(cluster.getId());
		shardTblDao.insert(shard);
		ShardTbl result = shardTblDao.findShard(clusterName, shard.getShardName(), ShardTblEntity.READSET_FULL);
		
		// dc-cluster-shards
		Map<Long,SetinelTbl> mapSetinels = generateSetinelsMap();
		List<DcClusterTbl> dcClusters = queryHandler.tryGet(new DalQuery<List<DcClusterTbl>>() {
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
					.setShardId(result.getId())
					.setDcClusterShardPhase(XpipeConsoleConstant.DEFAULT_DC_CLUSTER_PHASE);
				if(null != mapSetinels.get(dcCluster.getDcId())) {
					dcClusterShardProto.setSetinelId(mapSetinels.get(dcCluster.getDcId()).getSetinelId());
				}
				dcClusterShards.add(dcClusterShardProto);
			}
			dcClusterShardTblDao.insertBatch(dcClusterShards.toArray(new DcClusterShardTbl[dcClusterShards.size()]));
			
		}
		return result;
	}
	
	@DalTransaction
	public void deleteShardsBatch(List<ShardTbl> shards) throws DalException {
		if(null == shards) throw new DalException("Null cannot be deleted.");
		
		List<DcClusterShardTbl> relatedDcClusterShards = new LinkedList<DcClusterShardTbl>();
		for(final ShardTbl shard : shards) {
			List<DcClusterShardTbl> dcClusterShards = queryHandler.tryGet(new DalQuery<List<DcClusterShardTbl>>() {
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
		
		List<DcClusterShardTbl> relatedDcClusterShards = queryHandler.tryGet(new DalQuery<List<DcClusterShardTbl>>() {
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
		List<ShardTbl> shardNames = queryHandler.tryGet(new DalQuery<List<ShardTbl>>(){
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return shardTblDao.findAllByClusterName(clusterName, ShardTblEntity.READSET_NAME);
			}
		});
		if(null == shardNames) return;
		
		for(ShardTbl shardName : shardNames) {
			if(shardName.getShardName().equals(shard.getShardName())) {
				throw new BadRequestException("Duplicated shard name under same cluster.");
			}
		}
	}
	
	private Map<Long, SetinelTbl> generateSetinelsMap() {
		List<SetinelTbl> setinels = queryHandler.tryGet(new DalQuery<List<SetinelTbl>>() {
			@Override
			public List<SetinelTbl> doQuery() throws DalException {
				return setinelTblDao.findAll(SetinelTblEntity.READSET_FULL);
			}
		});
		
		Map<Long,SetinelTbl> mapSetinels = new HashMap<Long,SetinelTbl>();
		if(null != setinels) {
			for(SetinelTbl setinel : setinels) {
				mapSetinels.put(setinel.getDcId(), setinel);
			}
		}
		return mapSetinels;
	}
	
	
	
}
