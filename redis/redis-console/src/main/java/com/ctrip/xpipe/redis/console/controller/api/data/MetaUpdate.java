package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.api.migration.DC_TRANSFORM_DIRECTION;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.*;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.ClusterShardCounter;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.stream.Collectors;

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
    protected KeeperAdvancedService keeperAdvancedService;

    @Autowired
    private MetaServerConsoleServiceManagerWrapper metaServerConsoleServiceManagerWrapper;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private SentinelBalanceService sentinelBalanceService;

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


    @RequestMapping(value = "/clusters", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage createCluster(@RequestBody ClusterCreateInfo outerClusterCreateInfo) {

        ClusterCreateInfo clusterCreateInfo = transform(outerClusterCreateInfo, DC_TRANSFORM_DIRECTION.OUTER_TO_INNER);

        logger.info("[createCluster]{}", clusterCreateInfo);
        List<DcTbl> dcs = new LinkedList<>();
        try {
            clusterCreateInfo.check();

            ClusterTbl clusterTbl = clusterService.find(clusterCreateInfo.getClusterName());
            if (clusterTbl != null) {
                return RetMessage.createFailMessage(String.format("cluster:%s already exist", clusterCreateInfo.getClusterName()));
            }
            for (String dcName : clusterCreateInfo.getDcs()) {
                DcTbl dcTbl = dcService.find(dcName);
                if (dcTbl == null) {
                    return RetMessage.createFailMessage("dc not exist:" + dcName);
                }
                dcs.add(dcTbl);
            }
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }

        ClusterModel clusterModel = new ClusterModel();
        ClusterType clusterType = ClusterType.lookup(clusterCreateInfo.getClusterType());
        OrganizationTbl organizationTbl;
        try {
            organizationTbl = getOrganizationTbl(clusterCreateInfo);
            clusterCreateInfo.check();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
        long activeDcId = clusterType.supportMultiActiveDC() ? 0 : dcs.get(0).getId();
        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(activeDcId)
                .setClusterName(clusterCreateInfo.getClusterName())
                .setClusterType(clusterCreateInfo.getClusterType())
                .setClusterDescription(clusterCreateInfo.getDesc())
                .setClusterAdminEmails(clusterCreateInfo.getClusterAdminEmails())
                .setOrganizationInfo(organizationTbl)
                .setClusterOrgName(organizationTbl.getOrgName())
        );


        try {
            clusterModel.setDcs(dcs);
            clusterService.createCluster(clusterModel);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    //synchronizelly delete cluster, including meta server
    @RequestMapping(value = "/cluster/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.DELETE)
    public RetMessage deleteCluster(@PathVariable String clusterName, @RequestParam(defaultValue = "true") boolean checkEmpty) {
        logger.info("[deleteCluster]{}", clusterName);
        try {
            List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs(clusterName);
            if (checkEmpty) {
                for (DcTbl dcTbl: dcTbls) {
                    String dcName = dcTbl.getDcName();
                    List<RedisTbl> redises = redisService.findAllRedisesByDcClusterName(dcName, clusterName);
                    if (!redises.isEmpty()) {
                        logger.info("[deleteCluster][{}] check empty fail for dc {}", clusterName, dcName);
                        return RetMessage.createFailMessage("cluster not empty in dc " + dcName);
                    }
                }
            }

            clusterService.deleteCluster(clusterName);
            for(DcTbl dcTbl : dcTbls) {
                try {
                    metaServerConsoleServiceManagerWrapper.get(dcTbl.getDcName()).clusterDeleted(clusterName);
                } catch (Exception e) {
                    logger.warn("[deleteCluster]", e);
                    return RetMessage.createFailMessage("[" + dcTbl.getDcName() + "]MetaServer fails" + e.getMessage());
                }
            }
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[deleteCluster]", e);
            return RetMessage.createFailMessage(e.getMessage());
        } finally {
            logger.info("[deleteCluster][end]");
        }

    }

    private OrganizationTbl getOrganizationTbl(ClusterCreateInfo clusterCreateInfo) {
        Long organizationId = clusterCreateInfo.getOrganizationId();
        if(organizationId == null) {
            throw new IllegalStateException("organizationId is required");
        }
        OrganizationTbl organizationTbl = organizationService
            .getOrganizationTblByCMSOrganiztionId(organizationId);
        // If not exists, pull from cms first
        if(organizationTbl == null) {
            organizationService.updateOrganizations();
            organizationTbl = organizationService
                .getOrganizationTblByCMSOrganiztionId(organizationId);
            if(organizationTbl == null) {
                throw new IllegalStateException("Organization Id: " + organizationId + ", could not be found");
            }
        }
        return organizationTbl;

    }

    private ClusterCreateInfo transform(ClusterCreateInfo clusterCreateInfo, DC_TRANSFORM_DIRECTION direction) {

        List<String> dcs = clusterCreateInfo.getDcs();
        List<String> trans = new LinkedList<>();

        for (String dc : dcs) {

            String transfer = direction.transform(dc);
            if (!Objects.equals(transfer, dc)) {
                logger.info("[transform]{}->{}", dc, transfer);
            }
            trans.add(transfer);
        }
        clusterCreateInfo.setDcs(trans);
        return clusterCreateInfo;
    }

    @RequestMapping(value = "/cluster", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage updateCluster(@RequestBody ClusterCreateInfo clusterInfo) {
        return updateSingleCluster(clusterInfo);
    }

    @RequestMapping(value = "/cluster/exchangename", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage clusterExchangeName(@RequestBody ClusterExchangeNameInfo exchangeNameInfo) {
        ClusterTbl formerClusterTbl = null;
        ClusterTbl latterClusterTbl = null;

        try {
           exchangeNameInfo.check();

            formerClusterTbl = clusterService.find(exchangeNameInfo.getFormerClusterName());
            if (formerClusterTbl == null) {
                return RetMessage.createFailMessage("former cluster not exist");
            }

            if (formerClusterTbl.getId() != exchangeNameInfo.getFormerClusterId()) {
                return RetMessage.createFailMessage("former cluster id & name not match");
            }

            latterClusterTbl = clusterService.find(exchangeNameInfo.getLatterClusterName());
            if (latterClusterTbl == null) {
                return RetMessage.createFailMessage("latter cluster not exist");
            }

            if (latterClusterTbl.getId() != exchangeNameInfo.getLatterClusterId()) {
                return RetMessage.createFailMessage("latter cluster id & name not match");
            }

            clusterService.exchangeName(exchangeNameInfo.getFormerClusterId(),
                    exchangeNameInfo.getFormerClusterName(),
                    exchangeNameInfo.getLatterClusterId(),
                    exchangeNameInfo.getLatterClusterName());
        } catch (CheckFailException cfe) {
            return RetMessage.createFailMessage(cfe.getMessage());
        } catch (Exception e) {
            logger.error("{}", e);
            return RetMessage.createFailMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/clusters", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public RetMessage updateClusters(@RequestBody List<ClusterCreateInfo> clusterInfos) {
        for(ClusterCreateInfo clusterCreateInfo : clusterInfos) {
            RetMessage retMessage = updateSingleCluster(clusterCreateInfo);
            if(ObjectUtils.equals(retMessage.getState(), RetMessage.FAIL_STATE))
                return retMessage;
        }
        return RetMessage.createSuccessMessage();
    }

    private Long getOrganizationId(ClusterCreateInfo clusterCreateInfo) {
        OrganizationTbl organizationTbl = getOrganizationTbl(clusterCreateInfo);
        Long id = organizationTbl.getId();
        return id == null ? 0L : id;
    }

    private RetMessage updateSingleCluster(ClusterCreateInfo clusterInfo) {
        boolean needUpdate = false;
        try {
            ClusterTbl clusterTbl = clusterService.find(clusterInfo.getClusterName());
            if(clusterTbl == null) {
                String message = String.format("cluster not found: %s", clusterInfo.getClusterName());
                return RetMessage.createFailMessage(message);
            }
            Long clusterOrgId = getOrganizationId(clusterInfo);
            if(!ObjectUtils.equals(clusterTbl.getClusterOrgId(), clusterOrgId)) {
                needUpdate = true;
                clusterTbl.setClusterOrgId(clusterOrgId);
            }
            if(!ObjectUtils.equals(clusterTbl.getClusterAdminEmails(), clusterInfo.getClusterAdminEmails())) {
                needUpdate = true;
                clusterTbl.setClusterAdminEmails(clusterInfo.getClusterAdminEmails());
            }
            if(needUpdate) {
                clusterService.update(clusterTbl);
            } else {
                String message = String.format("No field changes for cluster: %s", clusterInfo.getClusterName());
                return RetMessage.createSuccessMessage(message);
            }
        } catch (Exception e) {
            logger.error("{}", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/clusters", method = RequestMethod.GET)
    public List<ClusterCreateInfo> getClusters(
            @RequestParam(required=false, defaultValue = "one_way", name = "type") String clusterType) throws CheckFailException {
        if (!ClusterType.isTypeValidate(clusterType)) {
            throw new CheckFailException("unknow cluster type " + clusterType);
        }

        logger.info("[getClusters]");

        List<ClusterTbl> allClusters = clusterService.findClustersWithOrgInfoByClusterType(clusterType);

        List<ClusterCreateInfo> result = new LinkedList<>();
        allClusters.forEach(clusterTbl -> {
            result.add(ClusterCreateInfo.fromClusterTbl(clusterTbl, dcService));
        });

        return transformFromInner(result);
    }

    @RequestMapping(value = "/cluster/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public ClusterCreateInfo getCluster(@PathVariable String clusterName) {

        logger.info("[getCluster]{}", clusterName);

        ClusterTbl clusterTbl = clusterService.findClusterAndOrg(clusterName);
        ClusterCreateInfo clusterCreateInfo = ClusterCreateInfo.fromClusterTbl(clusterTbl, dcService);

        return transform(clusterCreateInfo, DC_TRANSFORM_DIRECTION.INNER_TO_OUTER);
    }

    private List<ClusterCreateInfo> transformFromInner(List<ClusterCreateInfo> source) {

        List<ClusterCreateInfo> results = new LinkedList<>();
        source.forEach(clusterCreateInfo -> results.add(transform(clusterCreateInfo, DC_TRANSFORM_DIRECTION.INNER_TO_OUTER)));
        return results;
    }

    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
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
                shardService.createShard(clusterName, shardTbl, sentinelBalanceService.selectMultiDcSentinels());
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
    public List<ShardCreateInfo> getShards(@PathVariable String clusterName) {

        List<ShardTbl> allByClusterName = shardService.findAllByClusterName(clusterName);
        List<ShardCreateInfo> result = new LinkedList<>();

        allByClusterName.forEach(shardTbl -> result.add(
                new ShardCreateInfo(shardTbl.getShardName(), shardTbl.getSetinelMonitorName())));
        return result;
    }


    @RequestMapping(value = "/shards/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE,
            method = RequestMethod.POST)
    public RetMessage createShard(@PathVariable String clusterName, @PathVariable String shardName,
                                  @RequestBody List<RedisCreateInfo> redisCreateInfos) {

        logger.info("[createShard] Create Shard with redises: {} - {}", clusterName, shardName);

        try {
            createShardWithOnePost(clusterName, shardName, null, redisCreateInfos);
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
            createShardWithOnePost(clusterName, shardName, monitorName, redisCreateInfos);
            return RetMessage.createSuccessMessage("Successfully created shard");
        } catch (Exception e) {
            logger.error("[createShard]" + clusterName + "," + shardName, e);
            return RetMessage.createFailMessage(e.getMessage());
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
    private void createShardWithOnePost(String clusterName, String shardName, String monitorName,
                                          List<RedisCreateInfo> redisCreateInfos) throws Exception {

        // Pre-validate
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            throw new CheckFailException("Cluster could not be found");
        }

        validateRedisCreateInfo(redisCreateInfos);

        ShardTbl proto = new ShardTbl()
                .setSetinelMonitorName(monitorName)
                .setShardName(shardName);
        ShardTbl shardTbl = shardService.findOrCreateShardIfNotExist(clusterName, proto, sentinelBalanceService.selectMultiDcSentinels());

        // Fill in redis, keeper
        for(RedisCreateInfo redisCreateInfo : redisCreateInfos) {
            String dcId = outerDcToInnerDc(redisCreateInfo.getDcId());
            redisService.insertRedises(dcId, clusterName, shardName,
                    redisCreateInfo.getRedisAddresses());
            if (ClusterType.lookup(clusterTbl.getClusterType()).supportKeeper()) addKeepers(dcId, clusterName, shardTbl);
        }
    }

    @VisibleForTesting
    protected int addKeepers(String dcId, String clusterId, ShardTbl shardTbl) throws DalException {

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

        List<KeeperBasicInfo> bestKeepers = keeperAdvancedService.findBestKeepers(dcId,
                KEEPER_PORT_DEFAULT, (ip, port) -> true, clusterId);

        logger.info("[addKeepers]{},{},{},{}, {}", dcId, clusterId, shardTbl.getShardName(), bestKeepers);
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

    @PostMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}")
    public RetMessage bindDc(@PathVariable String clusterName, @PathVariable String dcName) {
        logger.info("[bindDc]{},{}", clusterName, dcName);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl || null == clusterTbl) {
            return RetMessage.createFailMessage("unknown " + (null == clusterTbl ? "cluster " + clusterName : "dc " + dcName));
        }
        List<DcTbl> dcTbls = dcService.findClusterRelatedDc(clusterName);
        if (dcTbls.stream().anyMatch(dc -> dc.getId() == dcTbl.getId())) {
            return RetMessage.createFailMessage("cluster has already contain dc " + dcName);
        }

        clusterService.bindDc(clusterName, dcName);
        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/clusters/" + CLUSTER_NAME_PATH_VARIABLE + "/dcs/{dcName}", method = RequestMethod.DELETE)
    public RetMessage unbindDc(@PathVariable String clusterName, @PathVariable String dcName) {
        logger.info("[unbindDc]{}, {}", clusterName, dcName);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl || null == clusterTbl) {
            return RetMessage.createFailMessage("unknown " + (null == clusterTbl ? "cluster " + clusterName : "dc " + dcName));
        }

        List<DcTbl> dcTbls = dcService.findClusterRelatedDc(clusterName);
        if (dcTbls.stream().noneMatch(dc -> dc.getId() == dcTbl.getId())) {
            return RetMessage.createFailMessage("cluster doesn't contain dc " + dcName);
        }
        if (clusterTbl.getActivedcId() == dcTbl.getId()) {
            return RetMessage.createFailMessage("not allow unbind active dc");
        }

        List<RedisTbl> redises = redisService.findAllRedisesByDcClusterName(dcName, clusterName);
        if (!redises.isEmpty()) {
            logger.info("[unbindDc][{}] check empty fail for dc {}", clusterName, dcName);
            return RetMessage.createFailMessage("cluster not empty in dc " + dcName);
        }

        clusterService.unbindDc(clusterName, dcName);
        return RetMessage.createSuccessMessage();
    }

}
