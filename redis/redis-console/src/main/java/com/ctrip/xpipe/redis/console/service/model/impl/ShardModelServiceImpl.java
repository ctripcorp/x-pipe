package com.ctrip.xpipe.redis.console.service.model.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Service
public class ShardModelServiceImpl implements ShardModelService{
	
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
	private ApplierService applierService;
	@Autowired
	private ReplDirectionService replDirectionService;
	@Autowired
	private KeeperContainerService keeperContainerService;

	@Override
	public List<ShardModel> getAllShardModel(String dcName, String clusterName) {
		List<ShardModel> shardModels = new ArrayList<ShardModel>(); 
		List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);
		
		if(null != shards) {
			for(ShardTbl shard : shards) {
				ShardModel shardModel = getShardModel(dcName, clusterName, shard.getShardName(), false, null);
				if (shardModel != null) shardModels.add(shardModel);
			}
		}
			
		return shardModels;
	}

	@Override
	public ShardModel getSourceShardModel(String clusterName, String srcDcName, String toDcName, String shardName) {
		ReplDirectionInfoModel replDirectionInfoModel =
				replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc(clusterName, srcDcName, toDcName);
		if (null == replDirectionInfoModel) return null;

		return getShardModel(toDcName, clusterName, shardName, true, replDirectionInfoModel);
	}

	@Override
	public ShardModel getShardModel(final String dcName, final String clusterName, final String shardName,
									boolean isSourceShard, final ReplDirectionInfoModel replDirectionInfoModel) {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(5);
		Future<DcTbl> future_dcInfo = fixedThreadPool.submit(new Callable<DcTbl>() {
			@Override
			public DcTbl call() throws Exception {
				return dcService.find(dcName);
			}
		});
		Future<ClusterTbl> future_clusterInfo = fixedThreadPool.submit(new Callable<ClusterTbl>() {
			@Override
			public ClusterTbl call() throws Exception {
				return clusterService.find(clusterName);
			}
		});
		Future<ShardTbl> future_shardInfo = fixedThreadPool.submit(new Callable<ShardTbl>() {
			@Override
			public ShardTbl call() throws Exception {
				return shardService.find(clusterName, shardName);
			}
		});
		Future<DcClusterTbl> future_dcClusterInfo = fixedThreadPool.submit(new Callable<DcClusterTbl>() {
			@Override
			public DcClusterTbl call() throws Exception {
				return dcClusterService.find(dcName, clusterName);
			}
		});
		Future<DcClusterShardTbl> future_dcClusterShardInfo = fixedThreadPool.submit(new Callable<DcClusterShardTbl>() {
			@Override
			public DcClusterShardTbl call() throws Exception {
				if (isSourceShard) {
					return dcClusterShardService.find(replDirectionInfoModel.getSrcDcName(), clusterName, shardName);
				} else {
					return dcClusterShardService.find(dcName, clusterName, shardName);
				}
			}
		});
		
		try {
			if(null == future_dcInfo.get() || null == future_clusterInfo.get() || null == future_shardInfo.get()
					|| null == future_dcClusterInfo.get() || future_dcClusterShardInfo.get() == null) {
				return null;
			}
			return getShardModel(future_dcInfo.get(),future_clusterInfo.get(),future_shardInfo.get(),
					future_dcClusterInfo.get(), future_dcClusterShardInfo.get(), isSourceShard, replDirectionInfoModel);
		} catch (ExecutionException e) {
			throw new DataNotFoundException("Cannot construct shard-model", e);
		} catch (InterruptedException e) {
			throw new ServerException("Concurrent execution failed.", e);
		} finally {
			fixedThreadPool.shutdown();
		}
	}
	
	private ShardModel getShardModel(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo, DcClusterTbl dcClusterInfo,
			 DcClusterShardTbl dcClusterShardInfo, boolean isSourceShard, ReplDirectionInfoModel replDirectionInfoModel) {
		if(null == dcInfo || null == clusterInfo || null == shardInfo
				|| null == dcClusterInfo || null == dcClusterShardInfo) {
			return null;
		}

		if (isSourceShard && dcClusterInfo.isGroupType()) {
			return null;
		}

		ShardModel shardModel = new ShardModel();
		shardModel.setShardTbl(shardInfo);
		Map<Long, Long> keeperContainerIdDcMap = keeperContainerService.keeperContainerIdDcMap();
		if (isSourceShard && replDirectionInfoModel != null) {
			addAppliersAndKeepersToSourceShard(shardModel, shardInfo.getId(), replDirectionInfoModel.getId(), dcInfo.getId(),
					dcClusterShardInfo.getDcClusterShardId(), keeperContainerIdDcMap);
		} else {
			addRedisesAndKeepersToNormalShard(shardModel, dcClusterShardInfo.getDcClusterShardId(),
														dcInfo.getId(), keeperContainerIdDcMap);
		}
		
		return shardModel;
	}

	private void addAppliersAndKeepersToSourceShard(ShardModel shardModel, long shardId, long replDirectionId,
													long dcId, long dcClusterShardId, Map<Long, Long> keeperContainerIdDcMap) {
		List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(shardId, replDirectionId);
        appliers.forEach(applier -> shardModel.addApplier(applier));

        List<RedisTbl> keepers = redisService.findAllByDcClusterShard(dcClusterShardId);
		if(null != keepers) {
			for (RedisTbl keeper : keepers) {
				if (keeper.getRedisRole().equals(XPipeConsoleConstant.ROLE_KEEPER) &&
						ObjectUtils.equals(Long.valueOf(dcId), keeperContainerIdDcMap.get(Long.valueOf(keeper.getKeepercontainerId())))) {
					shardModel.addKeeper(keeper);
				}
			}
		}
	}

	private void addRedisesAndKeepersToNormalShard(ShardModel shardModel, long dcClusterShardId, long dcId,
												   Map<Long, Long> keeperContainerIdDcMap) {
		List<RedisTbl> shard_redises = redisService.findAllByDcClusterShard(dcClusterShardId);
		if(null != shard_redises) {
			for(RedisTbl redis : shard_redises) {
				if(redis.getRedisRole().equals(XPipeConsoleConstant.ROLE_REDIS)) {
					shardModel.addRedis(redis);
				} else {
					if (ObjectUtils.equals(Long.valueOf(dcId),
							keeperContainerIdDcMap.get(Long.valueOf(redis.getKeepercontainerId())))) {
						shardModel.addKeeper(redis);
					}
				}
			}
		}
	}

}
