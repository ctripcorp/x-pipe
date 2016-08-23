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
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot construct dao.", e);
		}
	}
	
	@DalTransaction
	public ShardTbl createShard(String clusterName, ShardTbl shard) throws DalException{
		// TODO 
		ClusterTbl cluster = clusterTblDao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
		List<SetinelTbl> setinels = setinelTblDao.findAll(SetinelTblEntity.READSET_FULL);
		Map<Long,SetinelTbl> mapSetinels = new HashMap<Long,SetinelTbl>();
		for(SetinelTbl setinel : setinels) {
			mapSetinels.put(setinel.getDcId(), setinel);
		}
		
		// Check shard name
		List<ShardTbl> shards = shardTblDao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
		for(ShardTbl tmp_shard : shards) {
			if(tmp_shard.getShardName().equals(shard.getShardName())) {
				throw new BadRequestException("Duplicated shard name under same cluster.");
			}
		}
		
		ShardTbl proto = shardTblDao.createLocal();
		proto.setShardName(shard.getShardName());
		proto.setClusterId(cluster.getId());
		proto.setSetinelMonitorName(shard.getSetinelMonitorName());
		shardTblDao.insert(proto);
		ShardTbl result = shardTblDao.findShard(clusterName, shard.getShardName(), ShardTblEntity.READSET_FULL);
		
		List<DcClusterShardTbl> dcClusterShards = new LinkedList<DcClusterShardTbl>();
		List<DcClusterTbl> dcClusters = dcClusterTblDao.findAllByClusterId(cluster.getId(), DcClusterTblEntity.READSET_FULL);
		for(DcClusterTbl dcCluster : dcClusters) {
			DcClusterShardTbl dcClusterShardProto = dcClusterShardTblDao.createLocal();
			dcClusterShardProto.setDcClusterId(dcCluster.getDcClusterId());
			dcClusterShardProto.setShardId(result.getId());
			dcClusterShardProto.setSetinelId(mapSetinels.get(dcCluster.getDcId()).getSetinelId());
			
			dcClusterShards.add(dcClusterShardProto);
		}
		dcClusterShardTblDao.insertBatch((DcClusterShardTbl[]) dcClusterShards.toArray());
		
		return result;
	}
	
	public void deleteShardsBatch(List<ShardTbl> shards) throws DalException {
		if(null == shards) return;
		
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
		
		shardTblDao.deleteShardsBatch(shards.toArray(new ShardTbl[shards.size()]), ShardTblEntity.UPDATESET_FULL);
		
	}
	
	
}
