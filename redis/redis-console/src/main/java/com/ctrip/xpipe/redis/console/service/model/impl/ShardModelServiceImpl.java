package com.ctrip.xpipe.redis.console.service.model.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.DataNotFoundException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.keeper.command.*;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.impl.KeeperContainerServiceImpl;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.dianping.cat.utils.StringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.ctrip.xpipe.redis.checker.KeeperContainerService;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.checker.resource.Resource.*;
import static com.ctrip.xpipe.redis.console.keeper.AutoMigrateOverloadKeeperContainerAction.KEEPER_MIGRATION_ACTIVE_ROLLBACK_ERROR;

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
	private KeeperContainerServiceImpl keeperContainerServiceImpl;
    @Autowired
    private KeeperContainerService keeperContainerService;
    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;
	@Autowired
    private KeeperAdvancedService keeperAdvancedService;

    private static final Logger logger = LoggerFactory.getLogger(ShardModelServiceImpl.class);

    private final ExecutorService FIXED_THREAD_POOL = Executors
        .newFixedThreadPool(6, XpipeThreadFactory.create(getClass().getSimpleName()));

    @Resource(name = REDIS_COMMAND_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Resource(name = KEEPER_KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    private RetryCommandFactory<Object> retryCommandFactory;

    private RetryCommandFactory<Object> retryLongCommandFactory;

    @PostConstruct
    public void init() {
        retryCommandFactory = DefaultRetryCommandFactory.retryNTimes(scheduled, 3, 500);
        retryLongCommandFactory = DefaultRetryCommandFactory.retryNTimes(scheduled, 5, 1000);
    }

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

            Map<Long, Long> containerIdDcMap = keeperContainerServiceImpl.keeperContainerIdDcMap();
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
	public boolean migrateBackupKeeper(String dcName, String clusterName, ShardModel shardModel,
                                       String srcKeeperContainerIp, String targetKeeperContainerIp) {
        List<RedisTbl> newKeepers = keeperAdvancedService.getNewKeepers(dcName, clusterName, shardModel,
                                                                        srcKeeperContainerIp, targetKeeperContainerIp);
        return doMigrateKeepers(dcName, clusterName, shardModel, newKeepers);
    }

    @Override
    public boolean switchActiveKeeper(String activeIp, String backupIp, ShardModel shardModel) {
        List<RedisTbl> keepers = shardModel.getKeepers();
        if (keepers.size() != 2) {
            logger.warn("[switchMaster][keeperSizeMissMatch][{}:{}->{}] {}",
                     shardModel.getShardTbl().getShardName(), activeIp, backupIp, keepers.size());
            return false;
        }
        Endpoint activeKeeper = null, backUpKeeper = null;
        for (RedisTbl keeper : keepers) {
            if (keeper.getRedisIp().equals(activeIp)) {
                activeKeeper = new DefaultEndPoint(keeper.getRedisIp(), keeper.getRedisPort());
            } else {
                backUpKeeper = new DefaultEndPoint(keeper.getRedisIp(), keeper.getRedisPort());
            }
        }

        if (activeKeeper == null || backUpKeeper == null || !backupIp.equals(backUpKeeper.getHost())) {
            logger.warn("[switchMaster][keeperActiveMissMatch][{}:{}->{}]keepers1:{}:{},keepers2:{}:{}"
                    , shardModel.getShardTbl().getShardName(), activeIp, backupIp,
                    keepers.get(0).getRedisIp(), keepers.get(0).getRedisPort(), keepers.get(1).getRedisIp(), keepers.get(1).getRedisPort());
            return false;
        }
        Command<?> switchMasterCommand = retryCommandFactory.createRetryCommand(new KeeperResetCommand<>(activeKeeper.getHost(), shardModel.getShardTbl().getId(), keeperContainerService));
        Command<?> checkKeeperRoleCommand = retryCommandFactory.createRetryCommand(new CheckKeeperActiveCommand<>(keyedObjectPool, scheduled, backUpKeeper, true));
        SequenceCommandChain chain = new SequenceCommandChain(false, false);
        chain.add(switchMasterCommand);
        chain.add(checkKeeperRoleCommand);
        try {
            chain.execute().get();
            return chain.future().isSuccess();
        } catch (Exception e) {
            logger.error("[switchMaster][commandChainError][{}:{}->{}]", shardModel.getShardTbl().getShardName(), activeKeeper, backUpKeeper, e);
            return false;
        }  finally {
            try {
                keyedObjectPool.clear(activeKeeper);
                keyedObjectPool.clear(backUpKeeper);
            } catch (ObjectPoolException e) {
                logger.error("[switchMaster][keyedObjectPoolClearError][{}:{}->{}]", shardModel.getShardTbl().getShardName(), activeKeeper, backUpKeeper, e);
            }
        }
    }

    @Override
    public boolean migrateActiveKeeper(String dcName, String clusterName, ShardModel shardModel, String srcKeeperContainerIp, String targetKeeperContainerIp) throws Throwable {
        List<RedisTbl> oldKeepers = shardModel.getKeepers();
        List<RedisTbl> newKeepers = keeperAdvancedService.getNewKeepers(dcName, clusterName, shardModel,
                srcKeeperContainerIp, targetKeeperContainerIp);
        if (!doMigrateKeepers(dcName, clusterName, shardModel, newKeepers)) {
            logger.error("[migrateActiveKeeper][doMigrateKeepersFailed][{}:{}:{}]", dcName, clusterName, shardModel);
            return false;
        }
        Endpoint activeKeeper, backUpKeeper;
        if (newKeepers.get(0).getRedisIp().equals(targetKeeperContainerIp)) {
            activeKeeper = new DefaultEndPoint(newKeepers.get(1).getRedisIp(), newKeepers.get(1).getRedisPort());
            backUpKeeper = new DefaultEndPoint(newKeepers.get(0).getRedisIp(), newKeepers.get(0).getRedisPort());
        } else {
            backUpKeeper = new DefaultEndPoint(newKeepers.get(1).getRedisIp(), newKeepers.get(1).getRedisPort());
            activeKeeper = new DefaultEndPoint(newKeepers.get(0).getRedisIp(), newKeepers.get(0).getRedisPort());
        }
        SequenceCommandChain chain = null;
        try {
            Command<?> pingNewKeeperCommand = retryLongCommandFactory.createRetryCommand(new CheckKeeperConnectedCommand<>(keyedObjectPool, scheduled, backUpKeeper));
            pingNewKeeperCommand.execute().get();
            if (!pingNewKeeperCommand.future().isSuccess()) {
                logger.error("[migrateActiveKeeper][pingNewKeeperCommandFailed][{}:{}:{}]keeper:{}", dcName, clusterName, shardModel, backUpKeeper);
                return false;
            }
            Command<Object> replOffsetGetCommand = retryCommandFactory.createRetryCommand(new KeeperContainerReplOffsetGetCommand<>(keyedObjectPool, scheduled, activeKeeper));
            long activeMasterReplOffset = (long)replOffsetGetCommand.execute().get();
            if (!replOffsetGetCommand.future().isSuccess()) {
                logger.error("[migrateActiveKeeper][replOffsetGetCommandFailed][{}:{}:{}]keeper:{}", dcName, clusterName, shardModel, activeKeeper);
                return false;
            }
            Command<?> fullSyncJudgeRetryCommand = retryCommandFactory.createRetryCommand(new FullSyncJudgeCommand<>(keyedObjectPool, scheduled, activeKeeper, backUpKeeper, activeMasterReplOffset));
            Command<?> switchmasterCommand = retryCommandFactory.createRetryCommand(new KeeperResetCommand<>(activeKeeper.getHost(), shardModel.getShardTbl().getId(), keeperContainerService));
            Command<?> checkKeeperRoleCommand = retryCommandFactory.createRetryCommand(new CheckKeeperActiveCommand<>(keyedObjectPool, scheduled, backUpKeeper, true));
            chain = new SequenceCommandChain(false, false);
            chain.add(fullSyncJudgeRetryCommand);
            chain.add(switchmasterCommand);
            chain.add(checkKeeperRoleCommand);
            chain.execute().get();
            return getMigrateActiveKeeperResult(chain, dcName, clusterName, shardModel, oldKeepers);
        } catch (Exception e) {
            logger.error("[migrateActiveKeeper][doCommandException][{}:{}:{}]", dcName, clusterName, shardModel, e);
            return getMigrateActiveKeeperResult(chain, dcName, clusterName, shardModel, oldKeepers);
        } finally {
            try {
                keyedObjectPool.clear(activeKeeper);
                keyedObjectPool.clear(backUpKeeper);
            } catch (ObjectPoolException e) {
                logger.error("[migrateActiveKeeper][keyedObjectPoolClearError][{}, {}]", activeKeeper, backUpKeeper, e);
            }
        }

    }

    private boolean getMigrateActiveKeeperResult(SequenceCommandChain chain, String dcName, String clusterName, ShardModel shardModel, List<RedisTbl> oldKeepers) throws Throwable{
        if (chain == null || !chain.future().isSuccess()) {
            logger.info("[migrateActiveKeeper][doMigrateActiveKeeperRollback][{}:{}:{}]chain:{}, chain success:{}", dcName, clusterName, shardModel, chain, chain != null && chain.future().isSuccess());
            if (!doMigrateKeepers(dcName, clusterName, shardModel, oldKeepers)) {
                throw new Throwable(KEEPER_MIGRATION_ACTIVE_ROLLBACK_ERROR);
            }
            return false;
        }
        return true;
    }


    private boolean doMigrateKeepers(String dcName, String clusterName, ShardModel shardModel, List<RedisTbl> newKeepers) {
        if (newKeepers == null) {
            logger.error("[doMigrateKeepers][keeperIsNull][{}:{}:{}]", dcName, clusterName, shardModel.getShardTbl().getShardName());
            return false;
        }else if (newKeepers.size() == 2) {
            try {
                shardModel.setKeepers(newKeepers);
                logger.info("[doMigrateKeepers][construct][{},{},{}]{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModelToString(shardModel));
                redisService.updateRedises(dcName, clusterName, shardModel.getShardTbl().getShardName(), shardModel);
                logger.info("[doMigrateKeepers][success][{},{},{}]{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModelToString(shardModel));
                return true;
            } catch (Exception e) {
                logger.error("[doMigrateKeepers][failed][{},{},{}]{}", clusterName, dcName, shardModel.getShardTbl().getShardName(), shardModelToString(shardModel), e);
                return false;
            }
        } else {
            logger.error("[doMigrateKeepers][keeperSizeMissMatch][{}:{}:{}]keeper size:{}", dcName, clusterName, shardModel.getShardTbl().getShardName(), newKeepers.size());
            return false;
        }
    }

    private String shardModelToString(ShardModel shardModel) {
        StringBuilder builder = new StringBuilder();
        builder.append("redis:");
        shardModel.getRedises().forEach(redis -> builder.append(redis.getRedisIp()).append(":").append(redis.getRedisPort()).append(", "));
        builder.append("keepers:");
        shardModel.getKeepers().forEach(redis -> builder.append(redis.getRedisIp()).append(":").append(redis.getRedisPort()).append(", "));
        return builder.toString();
    }

    @VisibleForTesting
    public void setKeyedObjectPool(XpipeNettyClientKeyedObjectPool pool) {
        this.keyedObjectPool = pool;
    }


}
