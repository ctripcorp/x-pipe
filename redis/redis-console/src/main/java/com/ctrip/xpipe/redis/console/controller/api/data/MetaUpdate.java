package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.*;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.entity.DcClusterEntity;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.repository.DcClusterRepository;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.ClusterShardCounter;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.APPLIER_PORT_DEFAULT;
import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class MetaUpdate extends AbstractConsoleController {

    @Autowired
    protected ClusterService clusterService;

    @Autowired
    private ClusterMetaService clusterMetaService;

    @Autowired
    protected DcService dcService;

    @Autowired
    protected SentinelService sentinelService;

    @Autowired
    protected ShardService shardService;

    @Autowired
    protected MetaCache metaCache;

    @Autowired
    protected OrganizationService organizationService;

    @Autowired
    protected RedisService redisService;

    @Autowired
    protected ApplierService applierService;

    @Autowired
    protected KeeperAdvancedService keeperAdvancedService;

    @Autowired
    private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;

    @Autowired
    private SentinelBalanceService sentinelBalanceService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private DcClusterRepository dcClusterRepository;

    @Autowired
    private AzGroupCache azGroupCache;

    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;

    @Autowired
    private ReplDirectionService replDirectionService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private DcCache dcCache;

    @Autowired
    private ConsoleConfig config;

    @RequestMapping(value = "/stats", method = RequestMethod.GET)
    public Map<String, Integer> getStats() {

        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        ClusterShardCounter counter = new ClusterShardCounter();
        xpipeMeta.accept(counter);

        HashMap<String, Integer> counts = new HashMap<>();
        counts.put("clusterCount", counter.getClusterCount());
        counts.put("shardCount", counter.getShardCount());
        return counts;
    }


    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.POST)
    public RetMessage createShards(@PathVariable String clusterName, @RequestBody List<ShardCreateInfo> shards) {

        logger.info("[createShards]{}, {}", clusterName, shards);

        ClusterTbl clusterTbl = null;

        try {
            clusterTbl = clusterService.find(clusterName);
            if (clusterTbl == null) {
                return RetMessage.createFailMessage("cluster not exist");
            }
            for (ShardCreateInfo shardCreateInfo : shards) {
                shardCreateInfo.check();
            }
        } catch (CheckFailException e) {
            return RetMessage.createFailMessage(e.getMessage());
        }

        List<String> successShards = new LinkedList<>();
        List<String> failShards = new LinkedList<>();

        for (ShardCreateInfo shardCreateInfo : shards) {

            try {
                ShardTbl shardTbl = new ShardTbl()
                        .setSetinelMonitorName(shardCreateInfo.getShardMonitorName())
                        .setShardName(shardCreateInfo.getShardName());
                shardService.createShard(clusterName, shardTbl, sentinelBalanceService.selectMultiDcSentinels(ClusterType.lookup(clusterTbl.getClusterType())));
                successShards.add(shardCreateInfo.getShardName());
            } catch (Exception e) {
                logger.error("[createShards]" + clusterName + "," + shardCreateInfo.getShardName(), e);
                failShards.add(shardCreateInfo.getShardName());
            }
        }

        if (failShards.size() == 0) {
            return RetMessage.createSuccessMessage();
        } else {
            StringBuilder sb = new StringBuilder();
            if (successShards.size() > 0) {
                sb.append(String.format("success shards:%s", joiner.join(successShards)));
            }
            sb.append(String.format("fail shards:%s", joiner.join(failShards)));
            return RetMessage.createFailMessage(sb.toString());
        }
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public List<ShardCreateInfo> getShards(@PathVariable String clusterName) throws CheckFailException {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if(clusterTbl == null) {
            throw new CheckFailException("cluster not found:" + clusterName);
        }
        List<ShardTbl> allByClusterName = shardService.findAllByClusterName(clusterName);
        Map<Long, String> dcIdNameMap = dcService.dcNameMap();
        List<ShardCreateInfo> result = new LinkedList<>();
        for (ShardTbl shardTbl : allByClusterName) {
            List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.find(clusterName, shardTbl.getShardName());
            for (DcClusterShardTbl dcClusterShardTbl : dcClusterShardTbls) {
                result.add(new ShardCreateInfo(shardTbl.getId(), shardTbl.getShardName(), shardTbl.getSetinelMonitorName(),
                        dcIdNameMap.get(dcClusterShardTbl.getDcClusterInfo().getDcId())));
            }
        }

        return result;
    }


    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE,
            method = RequestMethod.POST)
    public RetMessage createShard(@PathVariable String clusterName, @PathVariable String shardName,
                                  @RequestBody List<RedisCreateInfo> redisCreateInfos) {

        logger.info("[createShard] Create Shard with redises: {} - {}", clusterName, shardName);

        try {
            doCreateShard(clusterName, shardName, null, null, redisCreateInfos);
            return RetMessage.createSuccessMessage("Successfully created shard");
        } catch (Exception e) {
            logger.error("[createShard]" + clusterName + "," + shardName, e);
            return RetMessage.createFailMessage(e.getMessage());
        }

    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE + "/{monitor}",
            method = RequestMethod.POST)
    public RetMessage createShard(@PathVariable String clusterName, @PathVariable String shardName,
                                  @PathVariable String monitorName,
                                  @RequestBody List<RedisCreateInfo> redisCreateInfos) {

        logger.info("[createShard] Create Shard with redises: {} - {}", clusterName, shardName);

        try {
            doCreateShard(clusterName, shardName, monitorName, null, redisCreateInfos);
            return RetMessage.createSuccessMessage("Successfully created shard");
        } catch (Exception e) {
            logger.error("[createShard]" + clusterName + "," + shardName, e);
            return RetMessage.createFailMessage(e.getMessage());
        }

    }

    @RequestMapping(value = "/shards/with/redises/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.POST)
    public RetMessage createShardsWithRedis(@PathVariable String clusterName, @RequestBody List<RedisCreateInfo> redisCreateInfos) {
        logger.info("[createShardsWithRedis] create Shard with redises: {} {}", clusterName, redisCreateInfos);

        Map<String, List<RedisCreateInfo>> shardName2RedisCreateInfoMap = new HashMap<>();
        for (RedisCreateInfo redisCreateInfo : redisCreateInfos) {
            shardName2RedisCreateInfoMap.computeIfAbsent(redisCreateInfo.getShardName(), ignore->new LinkedList<>()).add(redisCreateInfo);
        }
        List<String> successShards = new LinkedList<>();
        List<String> failShards = new LinkedList<>();

        Map<String, DcClusterTbl> dcName2DcClusterTblMap = new HashMap<>();
        for (Map.Entry<String, List<RedisCreateInfo>> shardName2RedisCreateInfoEntry : shardName2RedisCreateInfoMap.entrySet()) {
            String shardName = shardName2RedisCreateInfoEntry.getKey();
            List<RedisCreateInfo> shardRedisCreateInfos = shardName2RedisCreateInfoEntry.getValue();
            try {
                List<DcClusterTbl> dcClusterTbls = new LinkedList<>();
                for (RedisCreateInfo shardRedisCreateInfo : shardRedisCreateInfos) {
                    String dcId = shardRedisCreateInfo.getDcId();
                    DcClusterTbl dcClusterTbl = dcName2DcClusterTblMap.computeIfAbsent(dcId.toUpperCase(), ignore -> dcClusterService.find(dcId, clusterName));
                    if(dcClusterTbl == null) {
                        String message = String.format("dc %s not exist in cluster %s", dcId, clusterName);
                        return RetMessage.createFailMessage(message);
                    }
                    dcClusterTbls.add(dcClusterTbl);
                }
                doCreateShard(clusterName, shardName, null, dcClusterTbls, shardRedisCreateInfos);
                successShards.add(shardName);
            }catch (Exception e){
                logger.error("[createShardsWithRedis]" + clusterName + "," + shardName, e);
                failShards.add(shardName);
            }
        }

        if (failShards.size() == 0) {
            return RetMessage.createSuccessMessage();
        } else {
            StringBuilder sb = new StringBuilder();
            if (successShards.size() > 0) {
                sb.append(String.format("success shards:%s", joiner.join(successShards)));
            }
            sb.append(String.format("fail shards:%s", joiner.join(failShards)));
            return RetMessage.createFailMessage(sb.toString());
        }
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE,
            method = RequestMethod.DELETE)
    public RetMessage deleteShard(@PathVariable String clusterName, @PathVariable String shardName) {
        logger.info("[deleteShard] Delete Shard {} - {}", clusterName, shardName);
        try {
            if(clusterService.find(clusterName) == null) {
                return RetMessage.createSuccessMessage("Cluster already not exist");
            }
            if(shardService.find(clusterName, shardName) == null) {
                return RetMessage.createSuccessMessage("Shard already not exist");
            }
            shardService.deleteShard(clusterName, shardName);
            return RetMessage.createSuccessMessage("Successfully deleted shard");
        } catch (Exception e) {
            logger.error("[deleteShard] {}", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/sync", method = RequestMethod.DELETE)
    public RetMessage syncBatchDeleteShards(@PathVariable String clusterName, @RequestBody List<String> shardNames) {
        logger.info("[deleteShard] Delete Shards {} - {}", clusterName, shardNames);
        try {
            ClusterTbl clusterTbl = clusterService.find(clusterName);
            if (clusterTbl == null) {
                return RetMessage.createSuccessMessage("Cluster already not exist");
            }
            List<ShardTbl> allShards = shardService.findAllByClusterName(clusterName);
            if(!allShards.isEmpty()){
                // some already deleted, some not
                Set<String> allShardNames = allShards.stream().map(ShardTbl::getShardName).collect(Collectors.toSet());
                if(!allShardNames.containsAll(shardNames)){
                    return RetMessage.createSuccessMessage("Some shard already not exist");
                }
            }else {
                return RetMessage.createSuccessMessage("Shard already not exist");
            }
            shardService.deleteShards(clusterTbl, shardNames);
            List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs(clusterName);
            for (DcTbl dcTbl : dcTbls) {
                try {
                    metaServerConsoleServiceManagerWrapper.get(dcTbl.getDcName()).clusterModified(clusterName,
                            clusterMetaService.getClusterMeta(dcTbl.getDcName(), clusterName));
                } catch (Exception e) {
                    logger.warn("[modifiedCluster] notify dc {} MetaServer fail", dcTbl.getDcName(), e);
                    return RetMessage.createFailMessage("[" + dcTbl.getDcName() + "]MetaServer fails" + e.getMessage());
                }
            }
            return RetMessage.createSuccessMessage("Successfully deleted shard");
        } catch (Exception e) {
            logger.error("[deleteShard]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @DalTransaction
    private void doCreateShard(String clusterName, String shardName, String monitorName,
                               List<DcClusterTbl> dcClusterTbls, List<RedisCreateInfo> redisCreateInfos) throws Exception {

        // Pre-validate
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            throw new CheckFailException("Cluster could not be found");
        }

        validateRedisCreateInfo(redisCreateInfos);

        ShardTbl proto = new ShardTbl()
                .setSetinelMonitorName(monitorName)
                .setShardName(shardName);
        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(clusterName, proto, dcClusterTbls, sentinelBalanceService.selectMultiDcSentinels(ClusterType.lookup(clusterTbl.getClusterType())));

        // Fill in redis, keeper
        for(RedisCreateInfo redisCreateInfo : redisCreateInfos) {
            String dcId = outerDcToInnerDc(redisCreateInfo.getDcId());
            redisService.insertRedises(dcId, clusterName, shardName, redisCreateInfo.getRedisAddresses());
        }
        addKeepers(clusterTbl, shardTbl, redisCreateInfos);
    }

    protected void addKeepers(ClusterTbl clusterTbl, ShardTbl shardTbl, List<RedisCreateInfo> redisCreateInfos) throws Exception {
        if (ClusterType.lookup(clusterTbl.getClusterType()).supportKeeper()) {
            Map<String, DcClusterTbl> dcName2DcClusterTbl = new HashMap<>();
            Map<String, AzGroupClusterEntity> az2AzGroupClusterMap = new HashMap<>();

            List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(clusterTbl.getId());
            for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
                AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
                for (String az : azGroup.getAzs()) {
                    az2AzGroupClusterMap.put(az.toUpperCase(), azGroupCluster);
                }
            }

            for (RedisCreateInfo redisCreateInfo : redisCreateInfos) {
                String dcId = outerDcToInnerDc(redisCreateInfo.getDcId());
                String clusterName = clusterTbl.getClusterName();
                DcClusterTbl dcClusterTbl = dcName2DcClusterTbl.computeIfAbsent(dcId.toUpperCase(), ignore-> dcClusterService.find(dcId, clusterName));
                if (dcClusterTbl == null) {
                    throw new CheckFailException(String.format("dc %s not exist in cluster %s", redisCreateInfo.getDcId(), clusterName));
                }

                AzGroupClusterEntity azGroupCluster = az2AzGroupClusterMap.get(dcId.toUpperCase());
                if (azGroupCluster == null
                    || !ClusterType.isSameClusterType(azGroupCluster.getAzGroupClusterType(), ClusterType.SINGLE_DC)) {
                    doAddKeepers(dcId, clusterName, shardTbl, dcId);
                }
            }
        }
    }

    @VisibleForTesting
    protected int doAddKeepers(String dcId, String clusterId, ShardTbl shardTbl, String keeperDcId) throws DalException {

        List<RedisTbl> keepers = null;
        try {
            keepers = redisService.findKeepersByDcClusterShard(dcId, clusterId, shardTbl.getShardName());
        } catch (ResourceNotFoundException e) {
            logger.info("[addKeepers] no keepers on shard {}: {}", clusterId, shardTbl.getShardName());
        }

        if(keepers != null && !keepers.isEmpty()) {
            if (keepers.size() > 2) {
                throw new IllegalStateException("Keeper numbers should not be greater than 2");
            } else if (keepers.size() == 1) {
                try {
                    redisService.deleteKeepers(dcId, clusterId, shardTbl.getShardName());
                } catch (ResourceNotFoundException ignore) {
                    // should not catch this, as we already findRedisHealthCheckInstance keepers
                }
            } else {
                // if size == 2, do nothing
                return 0;
            }
        }

        List<KeeperBasicInfo> bestKeepers = keeperAdvancedService.findBestKeepers(keeperDcId,
                KEEPER_PORT_DEFAULT, (ip, port) -> true, clusterId);

        logger.info("[addKeepers]{},{},{},{}, {}", dcId, clusterId, shardTbl.getShardName(), bestKeepers, keeperDcId);
        try {
            return redisService.insertKeepers(dcId, clusterId, shardTbl.getShardName(), bestKeepers);
        } catch (ResourceNotFoundException e) {
            logger.warn("[addKeepers] {}", e);
        }
        return 0;
    }

    @VisibleForTesting
    protected void validateRedisCreateInfo(List<RedisCreateInfo> redisCreateInfos) {
        Set<String> dcIds = Sets.newHashSetWithExpectedSize(redisCreateInfos.size());
        for(RedisCreateInfo createInfo : redisCreateInfos) {
            if(!dcIds.add(createInfo.getDcId())) {
                throw new IllegalArgumentException(String.format("dc: %s appears more than two times",
                        createInfo.getDcId()));
            }
        }
    }

    @VisibleForTesting
    protected int addAppliers(String dcId, String clusterName, ShardTbl shardTbl, long replDirectionId) {

        List<ApplierTbl> applierTbls = applierService.findApplierTblByShardAndReplDirection(shardTbl.getId(), replDirectionId);
        if(applierTbls== null || applierTbls.isEmpty()) {
            logger.info("[addAppliers] no appliers on shard {}: {}", clusterName, shardTbl.getShardName());
        }

        if(applierTbls != null && !applierTbls.isEmpty()) {
            if (applierTbls.size() > 2) {
                throw new IllegalStateException("applier numbers should not be greater than 2");
            } else if (applierTbls.size() == 1) {
                applierService.deleteAppliers(shardTbl, replDirectionId);
            } else {
                // if size == 2, do nothing
                return 0;
            }
        }

        List<ApplierTbl> bestAppliers = applierService.findBestAppliers(dcId,
                        APPLIER_PORT_DEFAULT, (ip, port) -> true, clusterName)
                .stream()
                .map(applierBasicInfo -> new ApplierTbl()
                        .setIp(applierBasicInfo.getHost())
                        .setPort(applierBasicInfo.getPort())
                        .setContainerId(applierBasicInfo.getAppliercontainerId()))
                .collect(Collectors.toList());

        logger.info("[addAppliers]{},{},{},{}", dcId, clusterName, shardTbl.getShardName(), bestAppliers);
        return applierService.createAppliers(bestAppliers, shardTbl, replDirectionId);
    }

    @DalTransaction
    public void doCreateReplDirections(ClusterTbl clusterTbl, List<ReplDirectionInfoModel> replDirectionInfoModels) throws Exception {
        Set<String> replDcs = new HashSet<>();
        for (ReplDirectionInfoModel model : replDirectionInfoModels) {
            replDcs.add(model.getSrcDcName());
            replDcs.add(model.getFromDcName());
            replDcs.add(model.getToDcName());
        }
        List<DcClusterEntity> dcClusters = dcClusterRepository.selectByClusterId(clusterTbl.getId());
        Set<String> clusterDcs = dcClusters.stream()
            .map(dcCluster -> dcCache.find(dcCluster.getDcId()).getDcName())
            .collect(Collectors.toSet());
        if (!clusterDcs.containsAll(replDcs)) {
            replDcs.removeAll(clusterDcs);
            throw new CheckFailException(
                String.format("dcs %s not exist in cluster %s", replDcs, clusterTbl.getClusterName()));
        }

        Map<String, AzGroupClusterEntity> az2AzGroupClusterMap = new HashMap<>();
        List<AzGroupClusterEntity> azGroupClusters = azGroupClusterRepository.selectByClusterId(clusterTbl.getId());
        for (AzGroupClusterEntity azGroupCluster : azGroupClusters) {
            AzGroupModel azGroup = azGroupCache.getAzGroupById(azGroupCluster.getAzGroupId());
            for (String az : azGroup.getAzs()) {
                az2AzGroupClusterMap.put(az, azGroupCluster);
            }
        }

        ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
        for (ReplDirectionInfoModel replDirectionInfoModel : replDirectionInfoModels) {
            ReplDirectionTbl replDirectionTbl = replDirectionService.addReplDirectionByInfoModel(clusterTbl.getClusterName(), replDirectionInfoModel);
            List<ShardTbl> allSrcDcShards = shardService.findAllShardByDcCluster(replDirectionTbl.getSrcDcId(), clusterTbl.getId());
            List<ShardTbl> allToDcShards = shardService.findAllShardByDcCluster(replDirectionTbl.getToDcId(), clusterTbl.getId());

            AzGroupClusterEntity azGroupCluster = az2AzGroupClusterMap.get(replDirectionInfoModel.getFromDcName());
            if (!CollectionUtils.isEmpty(allSrcDcShards)) {
                for (ShardTbl shardTbl : allSrcDcShards) {
                    if (!CollectionUtils.isEmpty(allToDcShards)) {
                        addAppliers(replDirectionInfoModel.getToDcName(), clusterTbl.getClusterName(), shardTbl, replDirectionTbl.getId());
                    }
                    if (clusterType.supportKeeper() && azGroupCluster != null && ClusterType.isSameClusterType(
                        azGroupCluster.getAzGroupClusterType(), ClusterType.SINGLE_DC)) {
                        doAddKeepers(replDirectionInfoModel.getSrcDcName(), clusterTbl.getClusterName(), shardTbl, replDirectionInfoModel.getFromDcName());
                    }
                }
            }
        }
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/repl-direction", method = RequestMethod.POST)
    public RetMessage createReplDirections(@PathVariable String clusterName, @RequestBody List<ReplDirectionCreateInfo> replDirectionCreateInfos) {
        logger.info("[createReplDirections]{}, {}", clusterName, replDirectionCreateInfos);
        try {
            for (ReplDirectionCreateInfo replDirectionCreateInfo : replDirectionCreateInfos) {
                replDirectionCreateInfo.check();
            }
            ClusterTbl clusterTbl = clusterService.find(clusterName);
            if(clusterTbl == null) {
                return RetMessage.createFailMessage("unknown cluster " + clusterName);
            }
            List<ReplDirectionInfoModel> replDirectionInfoModels = new LinkedList<>();
            for (ReplDirectionCreateInfo replDirectionCreateInfo : replDirectionCreateInfos) {
                ReplDirectionInfoModel exist = replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc(clusterName, replDirectionCreateInfo.getSrcDcName(), replDirectionCreateInfo.getToDcName());
                if (exist != null) {
                    String message = String.format("cluster %s srcDc %s toDc %s repl direction already exist", clusterName, replDirectionCreateInfo.getSrcDcName(), replDirectionCreateInfo.getToDcName());
                    return RetMessage.createFailMessage(message);
                }
                ReplDirectionInfoModel replDirectionInfoModel = new ReplDirectionInfoModel()
                        .setClusterName(clusterName)
                        .setSrcDcName(replDirectionCreateInfo.getSrcDcName())
                        .setFromDcName(replDirectionCreateInfo.getFromDcName())
                        .setToDcName(replDirectionCreateInfo.getToDcName())
                        .setTargetClusterName(replDirectionCreateInfo.getTargetClusterName());
                replDirectionInfoModels.add(replDirectionInfoModel);
            }
            doCreateReplDirections(clusterTbl, replDirectionInfoModels);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[createReplDirections][fail] {}", replDirectionCreateInfos, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/repl-direction", method = RequestMethod.GET)
    public List<ReplDirectionCreateInfo> getReplDirections(@PathVariable String clusterName) {
        logger.info("[getReplDirections] {}", clusterName);
        List<ReplDirectionInfoModel> replDirectionInfoModels = replDirectionService.findAllReplDirectionInfoModelsByCluster(clusterName);
        List<ReplDirectionCreateInfo> replDirectionCreateInfos = replDirectionInfoModels
                .stream()
                .map(replDirectionInfoModel ->
                        new ReplDirectionCreateInfo()
                                .setSrcDcName(replDirectionInfoModel.getSrcDcName())
                                .setFromDcName(replDirectionInfoModel.getFromDcName())
                                .setToDcName(replDirectionInfoModel.getToDcName())
                                .setTargetClusterName(replDirectionInfoModel.getTargetClusterName()))
                .collect(Collectors.toList());
        return replDirectionCreateInfos;
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/repl-direction", method = RequestMethod.PUT)
    public RetMessage updateReplDirections(@PathVariable String clusterName, @RequestBody List<ReplDirectionCreateInfo> replDirectionCreateInfos) {
        logger.info("[updateReplDirections]{}, {}", clusterName, replDirectionCreateInfos);
        try {
            for (ReplDirectionCreateInfo replDirectionCreateInfo : replDirectionCreateInfos) {
                replDirectionCreateInfo.check();
            }
            ClusterTbl clusterTbl = clusterService.find(clusterName);
            if(clusterTbl == null) {
                return RetMessage.createFailMessage("unknown cluster " + clusterName);
            }
            List<ReplDirectionTbl> replDirectionTbls = new LinkedList<>();

            for (ReplDirectionCreateInfo replDirectionCreateInfo : replDirectionCreateInfos) {
                ReplDirectionTbl exist = replDirectionService.findByClusterAndSrcToDc(clusterName, replDirectionCreateInfo.getSrcDcName(), replDirectionCreateInfo.getToDcName());
                if (exist == null) {
                    String message = String.format("cluster %s srcDc %s toDc %s repl direction not exist", clusterName, replDirectionCreateInfo.getSrcDcName(), replDirectionCreateInfo.getToDcName());
                    return RetMessage.createFailMessage(message);
                }

                // only update targetClusterName
                ReplDirectionTbl replDirectionTbl = new ReplDirectionTbl()
                        .setId(exist.getId())
                        .setClusterId(clusterTbl.getId())
                        .setSrcDcId(exist.getSrcDcId())
                        .setFromDcId(exist.getFromDcId())
                        .setToDcId(exist.getToDcId())
                        .setTargetClusterName(replDirectionCreateInfo.getTargetClusterName());
                replDirectionTbls.add(replDirectionTbl);
            }
            replDirectionService.updateReplDirectionBatch(replDirectionTbls);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[updateReplDirections][fail] {}", replDirectionCreateInfos, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @DalTransaction
    public void doDeleteReplDirections(long clusterId, List<ReplDirectionTbl> replDirections) {
        replDirectionService.deleteReplDirectionBatch(replDirections);
        for (ReplDirectionTbl replDirection : replDirections) {
            List<ShardTbl> allSrcDcShards = shardService.findAllShardByDcCluster(replDirection.getSrcDcId(), clusterId);
            if(null!=allSrcDcShards && !allSrcDcShards.isEmpty()) {
                for (ShardTbl shardTbl : allSrcDcShards) {
                    applierService.deleteAppliers(shardTbl, replDirection.getId());
                }
            }
        }
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/repl-direction", method=RequestMethod.DELETE)
    public RetMessage deleteReplDirections(@PathVariable String clusterName, @RequestBody List<ReplDirectionCreateInfo> replDirectionCreateInfos) {
        logger.info("[deleteReplDirections]{}, {}", clusterName, replDirectionCreateInfos);
        try {
            for (ReplDirectionCreateInfo replDirectionCreateInfo : replDirectionCreateInfos) {
                replDirectionCreateInfo.check();
            }
            ClusterTbl clusterTbl = clusterService.find(clusterName);
            if(clusterTbl == null) {
                return RetMessage.createFailMessage("unknown cluster " + clusterName);
            }
            Map<String, Long> dcNameIdMap = dcService.dcNameIdMap();
            List<ReplDirectionTbl> replDirectionTbls = new LinkedList<>();
            for (ReplDirectionCreateInfo replDirectionCreateInfo : replDirectionCreateInfos) {
                ReplDirectionInfoModel exist = replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc(clusterName, replDirectionCreateInfo.getSrcDcName(), replDirectionCreateInfo.getToDcName());
                if (exist == null) {
                    String message = String.format("cluster %s srcDc %s toDc %s repl direction already not exist", clusterName, replDirectionCreateInfo.getSrcDcName(), replDirectionCreateInfo.getToDcName());
                    return RetMessage.createFailMessage(message);
                }

                ReplDirectionTbl replDirectionTbl = new ReplDirectionTbl()
                        .setId(exist.getId())
                        .setSrcDcId(dcNameIdMap.get(exist.getSrcDcName()));
                replDirectionTbls.add(replDirectionTbl);
            }
            doDeleteReplDirections(clusterTbl.getId(), replDirectionTbls);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[deleteReplDirections][fail]{}, {}", clusterName, replDirectionCreateInfos, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/repl-completion", method = RequestMethod.POST)
    public RetMessage completeReplicationByCluster(@PathVariable String clusterName) {
        logger.info("[completeReplicationByCluster]{}", clusterName);
        ClusterTbl cluster = clusterService.find(clusterName);
        if (cluster == null) {
            String msg = String.format("cluster %s does not exist", clusterName);
            logger.warn("[completeReplicationByCluster] fail {}", msg);
            return RetMessage.createFailMessage(msg);
        }

        List<ReplDirectionInfoModel> replDirections = replDirectionService.findAllReplDirectionInfoModelsByCluster(clusterName);
        if (replDirections == null || replDirections.isEmpty()) {
            String msg = String.format("cluster %s has no repl dirction", clusterName);
            logger.warn("[completeReplicationByCluster] fail {}", msg);
            return RetMessage.createFailMessage(msg);
        }

        try {
            replDirections.forEach(replDirection -> clusterService.completeReplicationByClusterAndReplDirection(cluster, replDirection));
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[completeReplicationByCluster][fail] {}", clusterName, e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @VisibleForTesting
    void setConfig(ConsoleConfig config) {
        this.config = config;
    }
}
