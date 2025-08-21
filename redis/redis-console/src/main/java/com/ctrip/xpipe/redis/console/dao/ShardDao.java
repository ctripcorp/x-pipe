package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.google.common.collect.Sets;
import jakarta.annotation.PostConstruct;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import java.util.*;


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

	public ShardTbl createShard(String clusterName, ShardTbl shard) {
		// shard basic
		shard.setSetinelMonitorName(shard.getShardName().trim());
		validateShard(clusterName, shard);
		return queryHandler.handleQuery(() -> insertShard(clusterName, shard));
	}

	public List<ShardTbl> queryAllShardsByClusterId(Long clusterId) {
		if (clusterId == null) {
			return Collections.emptyList();
		}
		return queryHandler.handleQuery(() -> shardTblDao.findAllByClusterId(clusterId, ShardTblEntity.READSET_FULL));
	}

	public List<ShardTbl> queryAllShardsByClusterName(String clusterName) {
        return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>(){
            @Override
            public List<ShardTbl> doQuery() throws DalException {
                return shardTblDao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
            }
        });
    }

    public Set<String> queryAllShardMonitorNames() {
        List<ShardTbl> shards = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
            @Override
            public List<ShardTbl> doQuery() throws DalException {
                return shardTblDao.queryAllMonitorNames(ShardTblEntity.READSET_MONITOR_NAME);
            }
        });
        Set<String> monitorNames = Sets.newHashSetWithExpectedSize(shards.size());
        shards.forEach(shardTbl -> monitorNames.add(shardTbl.getSetinelMonitorName()));

        return monitorNames;
    }

	@DalTransaction
	public void deleteShardsBatch(List<ShardTbl> shards) throws DalException {
		if(null == shards || shards.isEmpty()) {
			logger.warn("[deleteShardsBatch] Empty shards: {}", shards);
			return;
		}
		
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
		queryHandler.handleBatchDelete(new DalQuery<int[]>() {
			@Override
			public int[] doQuery() throws DalException {
				return shardTblDao.deleteShardsBatch(shards.toArray(new ShardTbl[shards.size()]),
						ShardTblEntity.UPDATESET_FULL);
			}
		}, true);
	}
	
	@DalTransaction
	public void deleteShardsBatch(final ShardTbl shard) throws DalException {
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

		queryHandler.handleDelete(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return shardTblDao.deleteShard(proto, ShardTblEntity.UPDATESET_FULL);
			}
		}, true);
	}
	
	public void validateShard(final String clusterName, ShardTbl shard) {
		// validate monitor name
		if (!shard.getShardName().equals(shard.getSetinelMonitorName())) {
			throw new BadRequestException("Monitor name should be exact same with shard name");
		}

		// validate shard name
		List<ShardTbl> shards = queryHandler.handleQuery(new DalQuery<List<ShardTbl>>(){
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return shardTblDao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
			}
		});
		if(null == shards) return;
		
		for(ShardTbl shardTbl : shards) {
			if(shardTbl.getShardName().trim().equals(shard.getShardName().trim())) {
				throw new BadRequestException("Duplicated shard name under same cluster.");
			}
			if(shardTbl.getSetinelMonitorName().trim().equals(shard.getSetinelMonitorName().trim())) {
				throw new BadRequestException("Duplicated sentinel monitor name under same cluster.");
			}
		}
	}

	public void insertShard(ShardTbl proto) {
		queryHandler.handleInsert(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				return shardTblDao.insert(proto);
			}
		});
	}

	@DalTransaction
	public ShardTbl insertShard(String clusterName, ShardTbl shard) throws DalException{

		final ClusterTbl cluster = clusterTblDao.findClusterByClusterName(clusterName, ClusterTblEntity.READSET_FULL);
		shard.setClusterId(cluster.getId());
		shard.setShardName(shard.getShardName().trim());
		shard.setSetinelMonitorName(shard.getSetinelMonitorName().trim());
		insertShard(shard);

		return shard;
	}
}
