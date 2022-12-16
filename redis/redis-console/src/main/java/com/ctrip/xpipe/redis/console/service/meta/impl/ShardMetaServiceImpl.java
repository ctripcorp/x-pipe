package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.AbstractMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.meta.ShardMetaService;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaQueryVO;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
@Service
public class ShardMetaServiceImpl extends AbstractMetaService implements ShardMetaService {
	@Autowired
	private DcService dcService;
	@Autowired
	private ClusterService clusterService;
	@Autowired
	private ShardService shardService;
	@Autowired
	private RedisService redisService;
	@Autowired
	private DcClusterService dcClusterService;
	@Autowired
	private DcClusterShardService dcClusterShardService;
	@Autowired 
	private RedisMetaService redisMetaService;
	@Autowired
	private ApplierService applierService;
	
	@Override
	public ShardMeta loadShardMeta(ClusterMeta clusterMeta,ClusterTbl clusterTbl, ShardTbl shardTbl, DcMetaQueryVO dcMetaQueryVO) {
		ShardMeta shardMeta = new ShardMeta();
		if(null == clusterTbl || null == shardTbl) return shardMeta;
		
		shardMeta.setId(shardTbl.getShardName());
		shardMeta.setDbId(shardTbl.getId());
 		shardMeta.setSentinelId(dcMetaQueryVO.getDcClusterShardMap().get(Pair.of(clusterTbl.getClusterName(), shardTbl.getShardName())).getSetinelId());
		shardMeta.setSentinelMonitorName(shardTbl.getSetinelMonitorName());
		for(RedisTbl redis : dcMetaQueryVO.getRedisMap().get(clusterTbl.getClusterName()).get(shardTbl.getShardName())) {
			if(redis.getRedisRole().equals("keeper")) {
				shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redis));
			} else {
				shardMeta.addRedis(redisMetaService.getRedisMeta(shardMeta, redis));
			}
		}
		
		shardMeta.setParent(clusterMeta);
		return shardMeta;
	}

	@Override
	public ShardMeta getShardMeta(final String dcName, final String clusterName, final String shardName,
								  Map<Long, Long> keeperContainerId2DcMap) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
		
		Future<DcTbl> future_dcInfo = fixedThreadPool.submit(new Callable<DcTbl>() {
			@Override
			public DcTbl call() throws DalException {
				return dcService.find(dcName);
			}
		});
		Future<ClusterTbl> future_clusterInfo = fixedThreadPool.submit(new Callable<ClusterTbl>() {
			@Override
			public ClusterTbl call() throws DalException {
				return clusterService.find(clusterName);
			}
		});
		Future<ShardTbl> future_shardInfo = fixedThreadPool.submit(new Callable<ShardTbl>() {
			@Override
			public ShardTbl call() throws DalException {
				return shardService.find(clusterName, shardName);
			}
		});
		Future<DcClusterTbl> future_dcClusterInfo = fixedThreadPool.submit(new Callable<DcClusterTbl>() {
			@Override
			public DcClusterTbl call() throws DalException {
				return dcClusterService.find(dcName, clusterName);
			}
		});
		Future<DcClusterShardTbl> future_dcClusterShardInfo = fixedThreadPool.submit(new Callable<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl call() throws DalException {
				return dcClusterShardService.find(dcName, clusterName, shardName);
			}
		});
		
		try {
			ShardTbl shardInfo = future_shardInfo.get();
			if (null == shardInfo) throw new DataNotFoundException("Cannot find shard-tbl " + shardName);

			if(null == future_dcInfo.get() || null == future_clusterInfo.get()
					|| null == future_dcClusterInfo.get() || null == future_dcClusterShardInfo.get()) {
				return null;
			}
			return getShardMeta(future_dcInfo.get(),future_clusterInfo.get(),future_shardInfo.get(),
					future_dcClusterInfo.get(), future_dcClusterShardInfo.get(), keeperContainerId2DcMap, false);
		} catch (ExecutionException e) {
			throw new DataNotFoundException("Cannot construct shard-meta", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent execution failed.", e);
		} finally {
			fixedThreadPool.shutdown();
		}
	}
	
	@Override
	public ShardMeta getShardMeta(final DcTbl dcInfo, final ClusterTbl clusterInfo, final ShardTbl shardInfo,
								  Map<Long, Long> keeperContainerId2DcMap) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(2);
		Future<DcClusterTbl> future_dcClusterInfo = fixedThreadPool.submit(new Callable<DcClusterTbl>() {
			@Override
			public DcClusterTbl call() throws DalException {
				return dcClusterService.find(dcInfo.getDcName(), clusterInfo.getClusterName());
			}
		});
		Future<DcClusterShardTbl> future_dcClusterShardInfo = fixedThreadPool.submit(new Callable<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl call() throws DalException {
				return dcClusterShardService.find(dcInfo.getDcName(), clusterInfo.getClusterName(), shardInfo.getShardName());
			}
		});
		
		try {
			return getShardMeta(dcInfo, clusterInfo, shardInfo, future_dcClusterInfo.get(), future_dcClusterShardInfo.get(),
					keeperContainerId2DcMap, false);
		} catch (ExecutionException e) {
			throw new DataNotFoundException("Cannot construct shard-meta", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent execution failed.", e);
		}  finally {
			fixedThreadPool.shutdown();
		}
	}

	@Override
	public ShardMeta getSourceShardMeta(DcTbl srcDcInfo, DcTbl currentDcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, DcClusterTbl dcClusterInfo,
										Map<Long, Long> keeperContainerId2DcMap, long replId) {

		if (srcDcInfo == null || currentDcInfo == null) {
			logger.error("srcDcInfo or currentDcInfo null");
			return null;
		}

		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(2);

		Future<List<ApplierTbl>> futureAppliers = fixedThreadPool.submit( () ->
				applierService.findApplierTblByShardAndReplDirection(shardInfo.getId(), replId));

		Future<DcClusterShardTbl> futureDcClusterShardInfo = fixedThreadPool.submit(() ->
				dcClusterShardService.find(srcDcInfo.getDcName(), clusterInfo.getClusterName(), shardInfo.getShardName()));

		try {
			List<ApplierTbl> appliers = futureAppliers.get();
			DcClusterShardTbl dcClusterShardTbl = futureDcClusterShardInfo.get();
			ShardMeta result = getShardMeta(currentDcInfo, clusterInfo, shardInfo, dcClusterInfo, dcClusterShardTbl,
					keeperContainerId2DcMap, true);
			addAppliers(result, appliers, clusterInfo.getClusterName());
			return result;
		} catch (ExecutionException e) {
			throw new DataNotFoundException("Cannot construct source-shard-meta", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent source-shard-meta execution failed.", e);
		}  finally {
			fixedThreadPool.shutdown();
		}
	}

	@VisibleForTesting
	protected void addAppliers(ShardMeta shardMeta, List<ApplierTbl> appliers, String clusterName) {
		if (shardMeta == null) {
			return;
		}
		for (ApplierTbl applierTbl : appliers) {
			ApplierMeta applierMeta = new ApplierMeta();
			applierMeta.setActive(applierTbl.isActive());
			applierMeta.setIp(applierTbl.getIp());
			applierMeta.setPort(applierTbl.getPort());
			applierMeta.setApplierContainerId(applierTbl.getContainerId());
			applierMeta.setTargetClusterName(getTargetClusterName(applierTbl, clusterName));

			shardMeta.addApplier(applierMeta);
		}
	}

	private String getTargetClusterName(ApplierTbl applierTbl, String clusterName) {
		if (applierTbl.getReplDirectionInfo() == null || StringUtils.isEmpty(applierTbl.getReplDirectionInfo().getTargetClusterName())) {
			return clusterName;
		}
		return applierTbl.getReplDirectionInfo().getTargetClusterName();
	}

	private ShardMeta getShardMeta(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, DcClusterTbl dcClusterInfo,
								   DcClusterShardTbl dcClusterShardInfo, Map<Long, Long> keeperContainerId2DcMap, boolean underSource) {
		if (null == shardInfo) throw new IllegalArgumentException("shard-tbl can't be null");

		if(null == dcInfo || null == clusterInfo || null == dcClusterInfo || null == dcClusterShardInfo) {
			return null;
		}

		ShardMeta shardMeta = new ShardMeta(shardInfo.getShardName()).setDbId(shardInfo.getId());
		shardMeta.setId(shardInfo.getShardName());
		if (!underSource) {
			shardMeta.setSentinelId(dcClusterShardInfo.getSetinelId());
			shardMeta.setSentinelMonitorName(SentinelUtil.getSentinelMonitorName(clusterInfo.getClusterName(), shardInfo.getSetinelMonitorName(), dcInfo.getDcName()));
		}

		List<RedisTbl> shard_redises = redisService.findAllByDcClusterShard(dcClusterShardInfo.getDcClusterShardId());
		if(null != shard_redises) {
			for(RedisTbl redis : shard_redises) {
				if(redis.getRedisRole().equals("keeper")) {
					Long dcId = keeperContainerId2DcMap.get(redis.getKeepercontainerId());
					if (dcId != null && dcId == dcInfo.getId()) {
						addKeeperMeta(shardMeta, redis);
					}
				} else {
					if (!underSource) {
						addRedisMeta(shardMeta, redis);
					}
				}
			}
		}
		
		return shardMeta;
	}

	private void addRedisMeta(ShardMeta shardMeta, RedisTbl redisInfo) {
		shardMeta.addRedis(redisMetaService.getRedisMeta(shardMeta, redisInfo));
	}
	
	private void addKeeperMeta(ShardMeta shardMeta, RedisTbl redisInfo) {
		shardMeta.addKeeper(redisMetaService.getKeeperMeta(shardMeta, redisInfo));
	}

}
