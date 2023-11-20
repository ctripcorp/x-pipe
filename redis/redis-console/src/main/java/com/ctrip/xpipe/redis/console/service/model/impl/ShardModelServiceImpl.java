package com.ctrip.xpipe.redis.console.service.model.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.dianping.cat.utils.StringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
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
    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;
	@Autowired
    private KeeperAdvancedService keeperAdvancedService;

    private static final Logger logger = LoggerFactory.getLogger(ShardModelServiceImpl.class);

    private final ExecutorService FIXED_THREAD_POOL = Executors
        .newFixedThreadPool(6, XpipeThreadFactory.create(getClass().getSimpleName()));

	@Override
    public List<ShardModel> getAllShardModel(String dcName, String clusterName) {
        List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);

        if (CollectionUtils.isEmpty(shards)) {
            return new ArrayList<>();
        } else {
            return this.getMultiShardModel(dcName, clusterName, shards, false, null);
        }
	}

    @Override
    public List<ShardModel> getMultiShardModel(final String dcName, final String clusterName,
        final List<ShardTbl> shards, final boolean isSourceShard,
        final ReplDirectionInfoModel replDirectionInfoModel) {
        if (StringUtils.isEmpty(dcName) || StringUtils.isEmpty(clusterName) || CollectionUtils.isEmpty(shards)) {
            return new ArrayList<>();
        }

        Future<DcTbl> dcFuture = FIXED_THREAD_POOL.submit(() -> dcService.find(dcName));
        Future<ClusterTbl> clusterFuture = FIXED_THREAD_POOL.submit(() -> clusterService.find(clusterName));
        Future<DcClusterTbl> dcClusterFuture = FIXED_THREAD_POOL.submit(() -> dcClusterService.find(dcName, clusterName));

        List<Future<DcClusterShardTbl>> dcClusterShardFutures = new ArrayList<>();
        String srcName = isSourceShard ? replDirectionInfoModel.getSrcDcName() : dcName;
        shards.forEach(shard -> {
            Future<DcClusterShardTbl> dcClusterShardFuture = FIXED_THREAD_POOL.submit(() -> {
                DcClusterShardTbl dcClusterShard = dcClusterShardService.find(srcName, clusterName, shard.getShardName());
                return dcClusterShard;
            });
            dcClusterShardFutures.add(dcClusterShardFuture);
        });

        List<ShardModel> shardModels = new ArrayList<>();
        try {
            DcTbl dcInfo = dcFuture.get(6, TimeUnit.SECONDS);
            ClusterTbl clusterInfo = clusterFuture.get(6, TimeUnit.SECONDS);
            DcClusterTbl dcClusterInfo = dcClusterFuture.get(6, TimeUnit.SECONDS);
            if(null == dcInfo || null == clusterInfo || null == dcClusterInfo) {
                return shardModels;
            }

            Map<Long, Long> containerIdDcMap = keeperContainerService.keeperContainerIdDcMap();
            for (int i = 0; i < shards.size(); i++) {
                ShardTbl shardInfo = shards.get(i);
                Future<DcClusterShardTbl> dcClusterShardFuture = dcClusterShardFutures.get(i);
                try{
                    if (null == dcClusterShardFuture.get(4, TimeUnit.SECONDS)) {
                        continue;
                    }
                } catch (TimeoutException e) {
                    logger.warn("get shard timeout, dc: {}, cluster: {}, shard: {}",
                        dcInfo.getDcName(), clusterInfo.getClusterName(), shardInfo.getShardName());

                    ShardModel fakeModel = new ShardModel();
                    fakeModel.setShardTbl(shardInfo);
                    shardModels.add(fakeModel);
                    continue;
                }

                ShardModel shardModel = this.getShardModel(dcInfo, clusterInfo, shardInfo,
                    dcClusterInfo, dcClusterShardFuture.get(), isSourceShard,
                    replDirectionInfoModel, containerIdDcMap);
                if (null != shardModel) {
                    shardModels.add(shardModel);
                }
            }

            return shardModels;
        } catch (TimeoutException e) {
            throw new ServerException("Server busy, please try again later", e);
        } catch (ExecutionException e) {
            throw new DataNotFoundException("Cannot construct shard-model", e);
        } catch (InterruptedException e) {
            throw new ServerException("Concurrent execution failed.", e);
        }
    }

	@Override
	public ShardModel getSourceShardModel(String clusterName, String srcDcName, String toDcName, String shardName) {
		ReplDirectionInfoModel replDirectionInfoModel =
				replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc(clusterName, srcDcName, toDcName);
		if (null == replDirectionInfoModel) return null;

		return getShardModel(toDcName, clusterName, shardName, true, replDirectionInfoModel);
	}

	@Override
	public ShardModel getShardModel(final String dcName, final String clusterName,
        final String shardName, boolean isSourceShard,
        final ReplDirectionInfoModel replDirectionInfoModel) {
        ShardTbl shard = shardService.find(clusterName, shardName);
        if (null == shard) {
            return null;
        }
		List<ShardModel> shardModels = this.getMultiShardModel(dcName, clusterName,
            Collections.singletonList(shard), isSourceShard, replDirectionInfoModel);
        if (CollectionUtils.isEmpty(shardModels)) {
            return null;
        } else {
            return shardModels.get(0);
        }
	}

	private ShardModel getShardModel(DcTbl dcInfo, ClusterTbl clusterInfo, ShardTbl shardInfo,
        DcClusterTbl dcClusterInfo, DcClusterShardTbl dcClusterShardInfo, boolean isSourceShard,
        ReplDirectionInfoModel replDirectionInfoModel, Map<Long, Long> containerIdDcMap) {
		if(null == dcInfo || null == clusterInfo || null == shardInfo
            || null == dcClusterInfo || null == dcClusterShardInfo) {
			return null;
		}

		if (isSourceShard) {
            long azGroupClusterId = dcClusterInfo.getAzGroupClusterId();
            ClusterType azGroupType = azGroupClusterRepository.selectAzGroupTypeById(azGroupClusterId);
            if (azGroupType != ClusterType.SINGLE_DC) {
                return null;
            }
        }

		ShardModel shardModel = new ShardModel();
		shardModel.setShardTbl(shardInfo);
		if (isSourceShard && replDirectionInfoModel != null) {
			this.addAppliersAndKeepersToSourceShard(shardModel, shardInfo.getId(),
                replDirectionInfoModel.getId(), dcInfo.getId(),
                dcClusterShardInfo.getDcClusterShardId(), containerIdDcMap);
		} else {
			this.addRedisesAndKeepersToNormalShard(shardModel,
                dcClusterShardInfo.getDcClusterShardId(), dcInfo.getId(), containerIdDcMap);
		}

		return shardModel;
	}

	private void addAppliersAndKeepersToSourceShard(ShardModel shardModel, long shardId,
        long replDirectionId, long dcId, long dcClusterShardId, Map<Long, Long> containerIdDcMap) {
		List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(shardId, replDirectionId);
        appliers.forEach(shardModel::addApplier);

        List<RedisTbl> keepers = redisService.findAllByDcClusterShard(dcClusterShardId);
		if(null != keepers) {
            for (RedisTbl keeper : keepers) {
                Long containerDcId = containerIdDcMap.get(keeper.getKeepercontainerId());
                if (keeper.getRedisRole().equals(XPipeConsoleConstant.ROLE_KEEPER)
                    && ObjectUtils.equals(dcId, containerDcId)) {
					shardModel.addKeeper(keeper);
				}
			}
		}
	}

	private void addRedisesAndKeepersToNormalShard(ShardModel shardModel, long dcClusterShardId,
        long dcId, Map<Long, Long> containerIdDcMap) {
		List<RedisTbl> shardRedises = redisService.findAllByDcClusterShard(dcClusterShardId);
		if(null != shardRedises) {
			for(RedisTbl redis : shardRedises) {
				if(redis.getRedisRole().equals(XPipeConsoleConstant.ROLE_REDIS)) {
					shardModel.addRedis(redis);
				} else {
                    if (ObjectUtils.equals(dcId, containerIdDcMap.get(redis.getKeepercontainerId()))) {
                        shardModel.addKeeper(redis);
					}
				}
			}
		}
	}

	@Override
	public boolean migrateShardKeepers(String dcName, String clusterName, ShardModel shardModel,
                                       String srcKeeperContainerIp, String targetKeeperContainerIp) {
        List<RedisTbl> newKeepers = keeperAdvancedService.getNewKeepers(dcName, clusterName, shardModel,
                                                                        srcKeeperContainerIp, targetKeeperContainerIp);
        if (newKeepers == null) {
            logger.debug("[migrateKeepers] no need to replace keepers");
            return false;
        }else if (newKeepers.size() == 2) {
            return doMigrateKeepers(shardModel, newKeepers, clusterName, dcName, shardModel.getShardTbl().getShardName());
        } else {
            logger.info("[migrateKeepers] fail to migrate keepers with unexpected newKeepers {}", newKeepers);
            return false;
        }
    }

    private boolean doMigrateKeepers(ShardModel shardModel, List<RedisTbl> newKeepers, String clusterName,
                                     String dcName, String shardName) {
        try {
            shardModel.setKeepers(newKeepers);
            logger.info("[Update Redises][construct]{},{},{},{}", clusterName, dcName, shardName, shardModel);
            redisService.updateRedises(dcName, clusterName, shardName, shardModel);
            logger.info("[Update Redises][success]{},{},{},{}", clusterName, dcName, shardName, shardModel);
            return true;
        } catch (Exception e) {
            logger.error("[Update Redises][failed]{},{},{},{}", clusterName, dcName, shardName, shardModel, e);
            return false;
        }
    }

}
