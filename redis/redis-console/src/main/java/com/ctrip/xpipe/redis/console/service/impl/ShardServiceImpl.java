package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class ShardServiceImpl extends AbstractConsoleService<ShardTblDao> implements ShardService {
	
	@Autowired
	private DcService dcService;
	@Autowired
	private ShardDao shardDao;
	@Autowired
	private ClusterMetaModifiedNotifier notifier;
	
	@Override
	public ShardTbl find(final long shardId) {
		return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return dao.findByPK(shardId, ShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public ShardTbl find(final String clusterName, final String shardName) {
		return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return dao.findShard(clusterName, shardName, ShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<ShardTbl> findAllByClusterName(final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return dao.findAllByClusterName(clusterName, ShardTblEntity.READSET_FULL);
			}
    	});
	}

	@Override
	public List<ShardTbl> findAllShardNamesByClusterName(final String clusterName) {
		return queryHandler.handleQuery(new DalQuery<List<ShardTbl>>() {
			@Override
			public List<ShardTbl> doQuery() throws DalException {
				return dao.findAllByClusterName(clusterName, ShardTblEntity.READSET_NAME);
			}
    	});
	}

	@Override
	public synchronized ShardTbl createShard(final String clusterName, final ShardTbl shard,
											 final Map<Long, SetinelTbl> sentinels) {
		return queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return shardDao.createShard(clusterName, shard, sentinels);
			}
    	});
	}

	@Override
	public synchronized ShardTbl findOrCreateShardIfNotExist(String clusterName, ShardTbl shard,
															 Map<Long, SetinelTbl> sentinels) {

		logger.info("[findOrCreateShardIfNotExist] Begin find or create shard: {}", shard);
		String monitorName = shard.getSetinelMonitorName();

		List<ShardTbl> shards = shardDao.queryAllShardsByClusterName(clusterName);

		Set<String> monitorNames = shardDao.queryAllShardMonitorNames();

		ShardTbl dupShardTbl = null;
		if(shards != null) {
			for (ShardTbl shardTbl : shards) {
				if (shardTbl.getShardName().equals(shard.getShardName())) {
					logger.info("[findOrCreateShardIfNotExist] Shard exist as: {} for input shard: {}",
							shardTbl, shard);
					dupShardTbl = shardTbl;
				}
			}
		}

		if(StringUtil.isEmpty(monitorName)) {
			return generateMonitorNameAndReturnShard(dupShardTbl, monitorNames, clusterName, shard, sentinels);

		} else {
			return compareMonitorNameAndReturnShard(dupShardTbl, monitorNames, clusterName, shard, sentinels);
		}
	}

	@Override
	public void deleteShard(final String clusterName, final String shardName) {
		final ShardTbl shard = queryHandler.handleQuery(new DalQuery<ShardTbl>() {
			@Override
			public ShardTbl doQuery() throws DalException {
				return dao.findShard(clusterName, shardName, ShardTblEntity.READSET_FULL);
			}
    	});
    	
    	if(null != shard) {
    		queryHandler.handleQuery(new DalQuery<Integer>() {
    			@Override
    			public Integer doQuery() throws DalException {
    				return shardDao.deleteShardsBatch(shard);
    			}
        	});
    	}
    	
    	/** Notify meta server **/
    	List<DcTbl> relatedDcs = dcService.findClusterRelatedDc(clusterName);
    	if(null != relatedDcs) {
    		for(DcTbl dc : relatedDcs) {
    			notifier.notifyClusterUpdate(dc.getDcName(), clusterName);
    		}
    	}
	}

	private ShardTbl generateMonitorNameAndReturnShard(ShardTbl dupShardTbl, Set<String> monitorNames,
													   String clusterName, ShardTbl shard,
													   Map<Long, SetinelTbl> sentinels) {
		String monitorName = null;
		if(dupShardTbl == null) {
			monitorName = monitorNames.contains(shard.getShardName())
					? clusterName + "-" + shard.getShardName()
					: shard.getShardName();
			if(monitorNames.contains(monitorName)) {
				logger.error("[findOrCreateShardIfNotExist] monitor name duplicated with {} and {}",
						shard.getShardName(), monitorName);
				throw new IllegalStateException(String.format("Both %s and %s is assigned as sentinel monitor name",
						shard.getShardName(), monitorName));
			}
			shard.setSetinelMonitorName(monitorName);
			try {
				return shardDao.insertShard(clusterName, shard, sentinels);
			} catch (DalException e) {
				throw new IllegalStateException(e);
			}
		} else {
			return dupShardTbl;
		}
	}

	private ShardTbl compareMonitorNameAndReturnShard(ShardTbl dupShardTbl, Set<String> monitorNames,
													  String clusterName, ShardTbl shard,
													  Map<Long, SetinelTbl> sentinels) {

		String monitorName = shard.getSetinelMonitorName();
		if(dupShardTbl == null) {
			if(monitorNames.contains(monitorName)) {
				logger.error("[findOrCreateShardIfNotExist] monitor name by post already exist {}", monitorName);
				throw new IllegalArgumentException(String.format("Shard monitor name %s already exist",
						monitorName));
			} else {
				try {
					return shardDao.insertShard(clusterName,shard, sentinels);
				} catch (DalException e) {
					throw new IllegalStateException(e);
				}
			}
		} else {
			if(!ObjectUtils.equals(dupShardTbl.getSetinelMonitorName(), monitorName)) {
				logger.error("[findOrCreateShardIfNotExist] shard monitor name in-consist with previous, {} -> {}",
						monitorName, dupShardTbl.getSetinelMonitorName());
				throw new IllegalArgumentException(String.format("Post shard monitor name %s diff from previous %s",
						monitorName, dupShardTbl.getSetinelMonitorName()));
			}
			return dupShardTbl;
		}
	}

}
