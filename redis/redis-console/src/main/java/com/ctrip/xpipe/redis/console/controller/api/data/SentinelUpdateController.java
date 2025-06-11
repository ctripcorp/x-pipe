package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.controller.result.GenericRetMessage;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel.SentinelMonitors;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.console.service.impl.ClusterServiceImpl.CLUSTER_DEFAULT_TAG;
import static com.ctrip.xpipe.spring.AbstractController.CLUSTER_NAME_PATH_VARIABLE;
import static com.ctrip.xpipe.spring.AbstractController.SHARD_NAME_PATH_VARIABLE;

/**
 * @author chen.zhu
 * <p>
 * Dec 27, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class SentinelUpdateController {

    @Autowired
    private SentinelGroupService sentinelGroupService;

    @Autowired
    public SentinelBalanceService sentinelBalanceService;

    @Autowired
    public DcClusterShardService dcClusterShardService;

    @Autowired
    public DcClusterService dcClusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private SentinelManager sentinelManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final JsonCodec jsonTool = new JsonCodec(true, true);

    @PostMapping("/rebalance/sentinels/{dcName}/execute")
    public RetMessage reBalanceSentinels(@PathVariable String dcName, @RequestParam(defaultValue = "true") boolean backupDcOnly, @RequestParam(defaultValue = CLUSTER_DEFAULT_TAG) String tag) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        if (backupDcOnly) {
            sentinelBalanceService.rebalanceBackupDcSentinel(dcTbl.getDcName(), tag);
        } else {
            sentinelBalanceService.rebalanceDcSentinel(dcTbl.getDcName(), ClusterType.ONE_WAY, tag);
        }
        return RetMessage.createSuccessMessage();
    }

    @PostMapping("/rebalance/sentinels/{dcName}/terminate")
    public RetMessage terminateBalanceSentinels(@PathVariable String dcName, @RequestParam(defaultValue = CLUSTER_DEFAULT_TAG) String tag) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        sentinelBalanceService.cancelCurrentBalance(dcTbl.getDcName(), ClusterType.ONE_WAY, tag);
        return RetMessage.createSuccessMessage();
    }

    @GetMapping("/rebalance/sentinels/{dcName}/process")
    public RetMessage getSentinelsBalanceProcess(@PathVariable String dcName, @RequestParam(defaultValue = CLUSTER_DEFAULT_TAG) String tag) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        SentinelBalanceTask balanceTask = sentinelBalanceService.getBalanceTask(dcTbl.getDcName(), ClusterType.ONE_WAY, tag);
        if (null == balanceTask) {
            return RetMessage.createSuccessMessage("no task");
        } else {
            Map<String, Object> ret = new HashMap<>();
            ret.put("name", balanceTask.getName());
            ret.put("targetUsage", balanceTask.getTargetUsages());
            ret.put("retainShards", balanceTask.getShardsWaitBalances());
            ret.put("finish", balanceTask.future().isDone());
            if (balanceTask.future().isDone() && null != balanceTask.future().cause()) {
                ret.put("err", balanceTask.future().cause().getMessage());
            }

            return GenericRetMessage.createGenericRetMessage(ret);
        }
    }

    @RequestMapping(value = "/sentinels", method = RequestMethod.POST)
    public RetMessage addSentinel(@RequestBody SentinelGroupModel sentinelGroupModel) {
        try {
            if (sentinelGroupModel.getTag() == null) {
                sentinelGroupModel.setTag(CLUSTER_DEFAULT_TAG);
            }
            sentinelGroupService.addSentinelGroup(sentinelGroupModel);
            return RetMessage.createSuccessMessage("Successfully create Sentinel");
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinels/{sentinelId}", method = RequestMethod.DELETE)
    public RetMessage deleteSentinel(@PathVariable Long sentinelId) {
        try {
            SentinelGroupModel setinelTbl = sentinelGroupService.findById(sentinelId);
            if (setinelTbl == null) {
                return RetMessage.createSuccessMessage("Sentinel already deleted");
            }
            sentinelGroupService.delete(sentinelId);
            return RetMessage.createSuccessMessage("Successfully deleted Sentinel");
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinels/{sentinelId}", method = RequestMethod.PATCH)
    public RetMessage rehealSentinel(@PathVariable Long sentinelId) {
        try {
            sentinelGroupService.reheal(sentinelId);
            return RetMessage.createSuccessMessage("Successfully reheal Sentinel");
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinels/active/{sentinelId}", method = RequestMethod.GET)
    public RetMessage getSentinelActiveStatus(@PathVariable Long sentinelId) {
        try {
            SentinelGroupModel setinelTbl = sentinelGroupService.findById(sentinelId);
            if (setinelTbl == null) {
                return RetMessage.createSuccessMessage("Sentinel does not exist or deleted");
            }
            return RetMessage.createSuccessMessage("Active status: " + setinelTbl.isActive());
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinels/active/{sentinelId}/{activeStatus}", method = RequestMethod.PUT)
    public RetMessage updateSentinelActiveStatus(@PathVariable Long sentinelId, @PathVariable Integer activeStatus) {
        try {
            SentinelGroupModel setinelTbl = sentinelGroupService.findById(sentinelId);
            if (setinelTbl == null) {
                return RetMessage.createSuccessMessage("Sentinel does not exist or deleted");
            }
            sentinelGroupService.updateActive(sentinelId, activeStatus);
            return RetMessage.createSuccessMessage("Successfully update ActiveStatus: " + activeStatus);
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinels/tag/{sentinelId}/{tag}", method = RequestMethod.PUT)
    public RetMessage updateSentinelTag(@PathVariable Long sentinelId, @PathVariable String tag) {
        try {
            SentinelGroupTbl sentinelGroupTbl = sentinelGroupService.updateTag(sentinelId, tag);
            return RetMessage.createSuccessMessage("Successfully update tag, sentinelGroupTbl : " + sentinelGroupTbl);
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = {"/sentinels/usage"}, method = RequestMethod.GET)
    public RetMessage sentinelUsage(@RequestParam(required = false) String clusterType, @RequestParam(required = false, defaultValue = "true") boolean includeCrossRegion) {
        logger.info("[sentinelUsage] begin to retrieve all sentinels' usage, includeCrossRegion: {}", includeCrossRegion);
        try {
            Map<String, SentinelUsageModel> sentinelUsage = sentinelGroupService.getAllSentinelsUsage(clusterType, includeCrossRegion);
            return GenericRetMessage.createGenericRetMessage(sentinelUsage);
        } catch (Exception e) {
            logger.error("[sentinelUsage]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @GetMapping("/meta")
    public XpipeMeta getMeta() {
        return sentinelGroupService.getMeta();
    }

    @RequestMapping(value = "/sentinel/address", method = RequestMethod.PUT)
    public RetMessage updateSentinelAddr(@RequestBody SentinelGroupModel sentinelGroupModel) {
        logger.info("[updateSentinelAddr][begin]");
        try {
            sentinelGroupService.updateSentinelGroupAddress(sentinelGroupModel);
            return RetMessage.createSuccessMessage(jsonTool.encode(sentinelGroupModel));
        } catch (Exception e) {
            logger.error("[updateSentinelAddr]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinel/monitor/{clusterName}", method = RequestMethod.DELETE)
    public RetMessage removeSentinelMonitor(@PathVariable String clusterName) {
        logger.info("[removeSentinelMonitor][begin]");
        try {
            return sentinelGroupService.removeSentinelMonitor(clusterName);
        } catch (Exception e) {
            logger.error("[removeSentinelMonitor]", e);
            return RetMessage.createFailMessage(e.getMessage());
        } finally {
            logger.info("[removeSentinelMonitor][end]");
        }
    }

    @RequestMapping(value = {"/sentinels/info/dc/{dcName}"}, method = RequestMethod.GET)
    public RetMessage getDcSentinels(@PathVariable String dcName) {
        logger.info("[getDcSentinels] begin to get dc {} sentinels", dcName);
        try {
            List<SentinelGroupModel> allDcSentinels = sentinelGroupService.findAllByDcName(dcName);
            return GenericRetMessage.createGenericRetMessage(allDcSentinels);
        } catch (Exception e){
            logger.error("[getDcSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = {"/sentinels/info/dc/{dcName}/clusterType/{clusterType}"}, method = RequestMethod.GET)
    public RetMessage getDcClusterTypeSentinels(@PathVariable String dcName, @PathVariable String clusterType) {
        logger.info("[getDcClusterTypeSentinels] begin to get dc {} clusterType {} sentinels", dcName, clusterType);
        try {
            List<SentinelGroupModel> allDcClusterTypeSentinels = sentinelGroupService.findAllByDcAndType(dcName, ClusterType.lookup(clusterType));
            return GenericRetMessage.createGenericRetMessage(allDcClusterTypeSentinels);
        } catch (Exception e){
            logger.error("[getDcClusterTypeSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = {"/dc/sentinels/{clusterType}", "/dc/sentinels"}, method = RequestMethod.GET)
    public RetMessage dcSentinelUsage(@RequestParam String dc, @PathVariable(required = false) String clusterType) {
        logger.info("[dcSentinelUsage] begin to retrieve {} sentinels' usage", dc);
        try {
            SentinelUsageModel sentinelUsage = sentinelGroupService.getAllSentinelsUsage(clusterType).get(dc.toUpperCase());
            return GenericRetMessage.createGenericRetMessage(sentinelUsage);
        } catch (Exception e) {
            logger.error("[dcSentinelUsage]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinel/shards", method = RequestMethod.GET)
    public RetMessage sentinelShards(@RequestParam String sentinelIp, @RequestParam int sentinelPort) {
        logger.info("[sentinelShards] begin to retrieve {}:{} monitor names", sentinelIp, sentinelPort);
        try {
            List<SentinelShardModel> sentinelShardModels = new ArrayList<>();
            String info = sentinelManager.infoSentinel(new Sentinel(String.format("%s:%d", sentinelIp, sentinelPort), sentinelIp, sentinelPort)).execute().get(2050, TimeUnit.MILLISECONDS);
            SentinelMonitors sentinelMonitors = SentinelMonitors.parseFromString(info);
            sentinelMonitors.getMasters().forEach(master -> {
                sentinelShardModels.add(new SentinelShardModel(SentinelUtil.getSentinelInfoFromMonitorName(master.getKey()).getShardName(), master.getValue()));
            });
            return GenericRetMessage.createGenericRetMessage(sentinelShardModels);
        } catch (Exception e) {
            logger.error("[sentinelShards]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/shard/sentinels", method = RequestMethod.GET)
    public RetMessage shardSentinels(@RequestParam String dc, @RequestParam String cluster, @RequestParam String shard) {
        logger.info("[shardSentinels] begin to retrieve {}:{} sentinels", dc, shard);
        try {
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dc, cluster, shard);
            if (dcClusterShardTbl == null)
                return RetMessage.createFailMessage(String.format("%s:%s:%s not found", dc, cluster, shard));

            return shardSentinels(dcClusterShardTbl);
        } catch (Exception e) {
            logger.error("[shardSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/redis/sentinels", method = RequestMethod.GET)
    public RetMessage redisSentinels(@RequestParam String ip, @RequestParam int port) {
        logger.info("[redisSentinels] begin to retrieve {}:{} sentinels", ip, port);
        try {
            RedisTbl redisTbl = redisService.findByIpPort(ip, port);
            if (redisTbl == null)
                return RetMessage.createFailMessage(String.format("%s:%d not found", ip, port));
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.findByPk(redisTbl.getDcClusterShardId());
            if (dcClusterShardTbl == null)
                return RetMessage.createFailMessage(String.format("dcClusterShard %d not found", redisTbl.getDcClusterShardId()));

            return shardSentinels(dcClusterShardTbl);
        } catch (Exception e) {
            logger.error("[redisSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    private RetMessage shardSentinels(DcClusterShardTbl dcClusterShardTbl) {

        ShardTbl shardTbl = shardService.find(dcClusterShardTbl.getShardId());
        SentinelGroupModel sentinelGroupModel = sentinelGroupService.findById(dcClusterShardTbl.getSetinelId());
        if (sentinelGroupModel == null)
            return RetMessage.createFailMessage(String.format("sentinel group id: %d not found", dcClusterShardTbl.getSetinelId()));

        List<DcClusterShardTbl> dcClusterShardTbls = Lists.newArrayList(dcClusterShardTbl);
        if (ClusterType.lookup(sentinelGroupModel.getClusterType()).equals(ClusterType.CROSS_DC)) {
            dcClusterShardTbls = dcClusterShardService.findByShardId(dcClusterShardTbl.getShardId());
        }

        List<RedisTbl> redisTbls = new ArrayList<>();
        for (DcClusterShardTbl dcClusterShard : dcClusterShardTbls) {
            redisTbls.addAll(redisService.findAllByDcClusterShard(dcClusterShard.getDcClusterShardId()));
        }
        HostPort master = null;
        for (RedisTbl redis : redisTbls) {
            if (redis.isMaster()) {
                master = new HostPort(redis.getRedisIp(), redis.getRedisPort());
                break;
            }
        }
        return GenericRetMessage.createGenericRetMessage(new SentinelShardModel(shardTbl.getShardName(), master).setSentinels(sentinelGroupModel.getSentinels().stream().map(sentinelInstanceModel -> new HostPort(sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort())).collect(Collectors.toList())));
    }

    @RequestMapping(value = "/shard/sentinels", method = RequestMethod.DELETE)
    public RetMessage removeDcShardSentinels(@RequestParam String dc, @RequestParam String cluster, @RequestParam String shard) {
        logger.info("[removeDcShardSentinels] begin to remove {}:{} sentinels", dc, shard);
        try {
            ClusterTbl clusterTbl=clusterService.find(cluster);
            if (clusterTbl == null)
                return RetMessage.createFailMessage(String.format("cluster %s not found", cluster));

            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dc, cluster, shard);
            if (dcClusterShardTbl == null)
                return RetMessage.createFailMessage(String.format("%s:%s:%s not found", dc, cluster, shard));

            removeShardSentinels(dc,clusterTbl,shard,dcClusterShardTbl);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[removeDcShardSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    private void removeShardSentinels(String dc, ClusterTbl clusterTbl, String shard, DcClusterShardTbl dcClusterShardTbl) throws DalException {
        String cluster = clusterTbl.getClusterName();
        String clusterType = clusterTbl.getClusterType();
        long originSentinelGroupId = dcClusterShardTbl.getSetinelId();

        List<DcClusterShardTbl> dcClusterShardTbls = Lists.newArrayList(dcClusterShardTbl);
        if (ClusterType.lookup(clusterType).equals(ClusterType.CROSS_DC)) {
            dc = consoleConfig.crossDcSentinelMonitorNameSuffix();
            dcClusterShardTbls = dcClusterShardService.find(cluster, shard);
        }
        for (DcClusterShardTbl dcClusterShard : dcClusterShardTbls) {
            dcClusterShardService.updateDcClusterShard(dcClusterShard.setSetinelId(0));
        }

        SentinelGroupModel sentinelGroupModel = sentinelGroupService.findById(originSentinelGroupId);
        if (sentinelGroupModel != null) {
            String sentinelMonitorName = SentinelUtil.getSentinelMonitorName(cluster, shard, dc);
            List<Sentinel> sentinels = sentinelGroupModel.getSentinels().stream().map(sentinelInstanceModel -> new Sentinel(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()), sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort())).collect(Collectors.toList());


            ParallelCommandChain chain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
            sentinels.forEach(sentinel -> {
                chain.add(sentinelManager.removeSentinelMonitor(sentinel, sentinelMonitorName));
            });
            chain.execute().getOrHandle(1000, TimeUnit.MILLISECONDS, throwable -> {
                logger.error("removeShardSentinels failed: {},{}", sentinelMonitorName, sentinels, throwable);
                return null;
            });

        }
    }

    //6、支持修改分片上的哨兵信息：
    @RequestMapping(value = "/shard/sentinels", method = RequestMethod.POST)
    public RetMessage updateDcShardSentinels(@RequestParam String dc, @RequestParam String cluster, @RequestParam String shard) {
        logger.info("[updateDcShardSentinels] begin to remove {}:{} sentinels", dc, shard);
        try {
            ClusterTbl clusterTbl = clusterService.find(cluster);
            if (clusterTbl == null)
                return RetMessage.createFailMessage(String.format("cluster %s not found",cluster));

            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dc, cluster, shard);
            if (dcClusterShardTbl == null)
                return RetMessage.createFailMessage(String.format("%s:%s:%s not found", dc, cluster, shard));

            return updateShardSentinels(dc, clusterTbl, shard, dcClusterShardTbl);
        } catch (Exception e) {
            logger.error("[updateDcShardSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "bind/shard/sentinels/{dcName}/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.POST)
    public RetMessage bindShardSentinels(@PathVariable String dcName, @PathVariable String clusterName, @PathVariable String shardName, @RequestBody SentinelMeta sentinelMeta) {
        logger.info("[bindShardSentinels] begin to bind shard {}:{}:{} with sentinels {}", dcName, clusterName, shardName, sentinelMeta);
        try {
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("bindShardSentinels: {}:{}:{}, sentinels:{}", dcName, clusterName, shardName, sentinelMeta.getAddress());
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "add/shard/sentinels/{dcName}/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE, method = RequestMethod.POST)
    public RetMessage addShardSentinels(@PathVariable String dcName, @PathVariable String clusterName, @PathVariable String shardName, @RequestBody HostPort master) {
        logger.info("[addShardSentinels] begin to add sentinels to shard {}:{}:{}, master:{} ", dcName, clusterName, shardName, master.toString());
        try {
            if (master.getHost() == null)
                return RetMessage.createFailMessage("master ip is null");

            ClusterTbl clusterTbl = clusterService.find(clusterName);
            if (clusterTbl == null) {
                return RetMessage.createFailMessage(String.format("cluster %s not found", clusterName));
            }

            ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
            SentinelGroupModel sentinelGroup = getSentinelGroup(clusterType, dcName, clusterName, shardName);

            String sentinelMonitorName = buildSentinelMonitorName(clusterType, dcName, clusterName, shardName);

            addSentinels(sentinelGroup, sentinelMonitorName, master);
            setSentinelsIfNeeded(clusterType, sentinelGroup, sentinelMonitorName);

            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("addShardSentinels: {}:{}:{}, master:{}", dcName, clusterName, shardName, master.toString());
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @PostMapping(value = "/bind/cluster/sentinels/{dcName}/" + CLUSTER_NAME_PATH_VARIABLE)
    public RetMessage addClusterSentinels(@PathVariable String dcName, @PathVariable String clusterName, @RequestBody SentinelMeta sentinelMeta) {
        try {
            logger.info("[bindSentinel][{}-{}] {}", dcName, clusterName, sentinelMeta.getId());
            List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.findAllByDcCluster(dcName, clusterName);
            SentinelGroupModel sentinelGroup = sentinelGroupService.findById(sentinelMeta.getId());
            if (null == sentinelGroup) {
                logger.debug("[bindSentinel][fail] no sentinel found");
                return RetMessage.createFailMessage("no sentinel " + sentinelMeta.getId());
            }
            for (DcClusterShardTbl dcClusterShardTbl: dcClusterShardTbls) {
                dcClusterShardTbl.setSetinelId(sentinelGroup.getSentinelGroupId());
                dcClusterShardService.updateDcClusterShard(dcClusterShardTbl);
            }
            return RetMessage.createSuccessMessage();
        } catch (Throwable th) {
            logger.warn("[bindSentinel][fail]", th);
            return RetMessage.createFailMessage(th.getMessage());
        }
    }

    void addSentinels(SentinelGroupModel sentinelGroup, String sentinelMonitorName, HostPort master) throws InterruptedException, ExecutionException, TimeoutException {
        ParallelCommandChain monitorChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        for (SentinelInstanceModel sentinelModel : sentinelGroup.getSentinels()) {
            Sentinel sentinel = new Sentinel(String.format("%s:%d", sentinelModel.getSentinelIp(), sentinelModel.getSentinelPort()), sentinelModel.getSentinelIp(), sentinelModel.getSentinelPort());
            monitorChain.add(sentinelManager.monitorMaster(sentinel, sentinelMonitorName, master, consoleConfig.getQuorum()));
            CatEventMonitor.DEFAULT.logEvent("Sentinel.Api.SentinelAdd", String.format("%s, %s, %s", sentinel.getName(), sentinelMonitorName, master.toString()));
        }
        monitorChain.execute().get(1000, TimeUnit.MILLISECONDS);
    }

    void setSentinelsIfNeeded(ClusterType clusterType, SentinelGroupModel sentinelGroup, String sentinelMonitorName) throws InterruptedException, ExecutionException, TimeoutException {
        Map<ClusterType, String[]> clusterTypeSentinelConfig = getClusterTypeSentinelConfig();
        String[] sentinelConfigs = clusterTypeSentinelConfig.get(clusterType);

        if (sentinelConfigs != null && sentinelConfigs.length != 0) {
            ParallelCommandChain sentinelSetChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
            for (SentinelInstanceModel sentinelModel : sentinelGroup.getSentinels()) {
                Sentinel sentinel = new Sentinel(String.format("%s:%d", sentinelModel.getSentinelIp(), sentinelModel.getSentinelPort()), sentinelModel.getSentinelIp(), sentinelModel.getSentinelPort());
                sentinelSetChain.add(sentinelManager.sentinelSet(sentinel, sentinelMonitorName, sentinelConfigs));
                CatEventMonitor.DEFAULT.logEvent("Sentinel.Api.SentinelSet", String.format("%s, %s", sentinel.getName(), sentinelMonitorName));
            }
            sentinelSetChain.execute().get(1000, TimeUnit.MILLISECONDS);
        }
    }

    SentinelGroupModel getSentinelGroup(ClusterType clusterType, String dcName, String clusterName, String shardName) {
        long sentinelGroupId;
        if (clusterType.equals(ClusterType.CROSS_DC)) {
            List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.find(clusterName, shardName);
            if (dcClusterShardTbls == null || dcClusterShardTbls.isEmpty())
                throw new XpipeRuntimeException(String.format("dc cluster shard not found by %s:%s:%s", dcName, clusterName, shardName));
            sentinelGroupId = dcClusterShardTbls.get(0).getSetinelId();
        } else {
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcName, clusterName, shardName);
            if (dcClusterShardTbl == null)
                throw new XpipeRuntimeException(String.format("dc cluster shard not found by %s:%s:%s", dcName, clusterName, shardName));
            sentinelGroupId = dcClusterShardTbl.getSetinelId();
        }

        if (sentinelGroupId == 0)
            throw new XpipeRuntimeException(String.format("no sentinel config for dc cluster shard %s:%s:%s", dcName, clusterName, shardName));

        SentinelGroupModel sentinelGroupModel = sentinelGroupService.findById(sentinelGroupId);
        if (sentinelGroupModel == null) {
            throw new XpipeRuntimeException(String.format("no sentinel group found by id:%d", sentinelGroupId));
        }

        return sentinelGroupModel;
    }

    String buildSentinelMonitorName(ClusterType clusterType, String dcName, String clusterName, String shardName) {
        if (clusterType.equals(ClusterType.CROSS_DC)) {
            return SentinelUtil.getSentinelMonitorName(clusterName, shardName, consoleConfig.crossDcSentinelMonitorNameSuffix());
        } else {
            return SentinelUtil.getSentinelMonitorName(clusterName, shardName, dcName);
        }
    }

    Map<ClusterType, String[]> getClusterTypeSentinelConfig() {
        Map<ClusterType, String[]> clusterTypeSentinelConfig = new HashMap<>();
        Map<String, String> configMap = consoleConfig.sentinelMasterConfig();
        configMap.forEach((k, v) -> {
            String[] sentinelConfigs = v.split("\\s*,\\s*");
            clusterTypeSentinelConfig.put(ClusterType.lookup(k), sentinelConfigs);
        });
        return clusterTypeSentinelConfig;
    }


    private RetMessage updateShardSentinels(String dcName, ClusterTbl clusterTbl, String shardName,
        DcClusterShardTbl dcClusterShardTbl) throws DalException {
        String clusterName = clusterTbl.getClusterName();
        ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());

        AzGroupClusterEntity azGroupCluster = azGroupClusterRepository.selectByClusterIdAndAz(clusterTbl.getId(), dcName);
        ClusterType azGroupType = azGroupCluster == null
            ? null : ClusterType.lookup(azGroupCluster.getAzGroupClusterType());
        ClusterType dcClusterType = azGroupType == null
            ? clusterType : azGroupType;

        SentinelGroupModel selected = sentinelBalanceService.selectSentinel(dcName, dcClusterType, clusterTbl.getTag());
        if (dcClusterShardTbl.getSetinelId() == selected.getSentinelGroupId())
            return RetMessage.createSuccessMessage("current sentinel is suitable, no change");

        List<DcClusterShardTbl> dcClusterShardTbls = Lists.newArrayList(dcClusterShardTbl);
        if (clusterType.equals(ClusterType.CROSS_DC)) {
            dcClusterShardTbls = dcClusterShardService.find(clusterName, shardName);
        }

        for (DcClusterShardTbl dcClusterShard : dcClusterShardTbls) {
            dcClusterShardService.updateDcClusterShard(dcClusterShard.setSetinelId(selected.getSentinelGroupId()));
        }

        return RetMessage.createSuccessMessage(String.format("sentinel changed to %s", selected.getSentinelsAddressString()));
    }

    @VisibleForTesting
    protected SetinelTbl convert2SentinelTbl(SentinelModel sentinelModel) {
        StringBuilder sb = new StringBuilder();
        for(HostPort hostPort : sentinelModel.getSentinels()) {
            sb.append(hostPort.getHost()).append(':').append(hostPort.getPort()).append(',');
        }
        String sentinels = sb.deleteCharAt(sb.length() - 1).toString();

        SetinelTbl proto = new SetinelTbl();
        proto.setSetinelAddress(sentinels);
        proto.setDeleted(false);
        proto.setSetinelDescription(sentinelModel.getDesc());
        proto.setDcId(dcService.find(sentinelModel.getDcName()).getId());

        return proto;
    }

}
