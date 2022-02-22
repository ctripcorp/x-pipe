package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.controller.result.GenericRetMessage;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.sentinel.SentinelMonitors;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author chen.zhu
 * <p>
 * Dec 27, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class SentinelUpdateController {

    @Autowired
    private SentinelService sentinelService;

    @Autowired
    private SentinelGroupService sentinelGroupService;

    @Autowired
    public SentinelBalanceService sentinelBalanceService;

    @Autowired
    public DcClusterShardService dcClusterShardService;

    @Autowired
    private DcService dcService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private SentinelManager sentinelManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final JsonCodec jsonTool = new JsonCodec(true, true);

    @PostMapping("/rebalance/sentinels/{dcName}/execute")
    public RetMessage reBalanceSentinels(@PathVariable String dcName, @RequestParam(defaultValue = "true") boolean backupDcOnly) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        if (backupDcOnly) {
            sentinelBalanceService.rebalanceBackupDcSentinel(dcTbl.getDcName());
        } else {
            sentinelBalanceService.rebalanceDcSentinel(dcTbl.getDcName(), ClusterType.ONE_WAY);
        }
        return RetMessage.createSuccessMessage();
    }

    @PostMapping("/rebalance/sentinels/{dcName}/terminate")
    public RetMessage terminateBalanceSentinels(@PathVariable String dcName) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        sentinelBalanceService.cancelCurrentBalance(dcTbl.getDcName(), ClusterType.ONE_WAY);
        return RetMessage.createSuccessMessage();
    }

    @GetMapping("/rebalance/sentinels/{dcName}/process")
    public RetMessage getSentinelsBalanceProcess(@PathVariable String dcName) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        SentinelBalanceTask balanceTask = sentinelBalanceService.getBalanceTask(dcTbl.getDcName(), ClusterType.ONE_WAY);
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
            sentinelGroupService.addSentinelGroup(sentinelGroupModel);
            return RetMessage.createSuccessMessage("Successfully create Sentinel");
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/bind/sentinels/{clusterType}", method = RequestMethod.POST)
    public RetMessage bindSentinels(@PathVariable String clusterType) {
        try {
            sentinelBalanceService.bindShardAndSentinelsByType(ClusterType.lookup(clusterType));
            return RetMessage.createSuccessMessage("Successfully bind Sentinel");
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

    @RequestMapping(value = "/sentinels/usage", method = RequestMethod.GET)
    public RetMessage sentinelUsage() {
        logger.info("[sentinelUsage] begin to retrieve all sentinels' usage");
        try {
            Map<String, SentinelUsageModel> sentienlUsage = sentinelGroupService.getAllSentinelsUsage();
            return GenericRetMessage.createGenericRetMessage(sentienlUsage);
        } catch (Exception e) {
            logger.error("[sentinelUsage]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
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

    //1、给出dc哨兵组信息，以及每组哨兵组当前的监控组数量
    @RequestMapping(value = "/dc/sentinels", method = RequestMethod.GET)
    public RetMessage dcSentinelUsage(@RequestParam String dc) {
        logger.info("[dcSentinelUsage] begin to retrieve {} sentinels' usage", dc);
        try {
            SentinelUsageModel sentinelUsage = sentinelGroupService.getAllSentinelsUsage().get(dc.toUpperCase());
            return GenericRetMessage.createGenericRetMessage(sentinelUsage);
        } catch (Exception e) {
            logger.error("[dcSentinelUsage]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    //2、给出单个哨兵组的监控分片详情
    @RequestMapping(value = "/sentinel/shards", method = RequestMethod.GET)
    public RetMessage sentinelShards(@RequestParam String sentinelIp, @RequestParam int sentinelPort) {
        logger.info("[sentinelShards] begin to retrieve {}:{} monitor names", sentinelIp, sentinelPort);
        try {
            List<SentinelShardModel> sentinelShardModels = new ArrayList<>();
            String info = sentinelManager.infoSentinel(new Sentinel(String.format("%s:%d", sentinelIp, sentinelPort), sentinelIp, sentinelPort));
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

    //3、给出单个机房分片上的监控哨兵组
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
        if (sentinelGroupModel.getClusterType().equalsIgnoreCase(ClusterType.CROSS_DC.name())) {
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

    //4、支持永久删除分片上的哨兵信息（需要后续自动加入）：将分片的sentinel_id设置为0
    @RequestMapping(value = "/redis/sentinels", method = RequestMethod.DELETE)
    public RetMessage removeRedisSentinel(@RequestParam String ip, @RequestParam int port) {
        logger.info("[removeRedisSentinel] begin to remove {}:{} sentinels", ip, port);
        try {
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.findAllByRedis(ip, port);
            if (dcClusterShardTbl == null)
                return RetMessage.createFailMessage(String.format("dcClusterShard not found by redis %s:%d", ip, port));

            removeShardSentinels(dcClusterShardTbl);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[removeRedisSentinel]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/shard/sentinels", method = RequestMethod.DELETE)
    public RetMessage removeDcShardSentinels(@RequestParam String dc, @RequestParam String cluster, @RequestParam String shard) {
        logger.info("[removeDcShardSentinels] begin to remove {}:{} sentinels", dc, shard);
        try {
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.findAllMeta(dc, cluster, shard);
            if (dcClusterShardTbl == null)
                return RetMessage.createFailMessage(String.format("%s:%s:%s not found", dc, cluster, shard));

            removeShardSentinels(dcClusterShardTbl);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[removeDcShardSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    private void removeShardSentinels(DcClusterShardTbl dcClusterShardTbl) throws DalException {
        String cluster = dcClusterShardTbl.getClusterInfo().getClusterName();
        String shard = dcClusterShardTbl.getShardInfo().getShardName();
        String dc = dcClusterShardTbl.getDcInfo().getDcName();
        List<DcClusterShardTbl> dcClusterShardTbls = Lists.newArrayList(dcClusterShardTbl);

        SentinelGroupModel sentinelGroupModel = sentinelGroupService.findById(dcClusterShardTbl.getSetinelId());
        if (sentinelGroupModel != null && sentinelGroupModel.getClusterType().equalsIgnoreCase(ClusterType.CROSS_DC.name())) {
            dc = consoleConfig.crossDcSentinelMonitorNameSuffix();
            dcClusterShardTbls = dcClusterShardService.find(cluster, shard);
        }

        for (DcClusterShardTbl dcClusterShard : dcClusterShardTbls) {
            dcClusterShardService.updateDcClusterShard(dcClusterShard.setSetinelId(0));
        }

        String sentinelMonitorName = SentinelUtil.getSentinelMonitorName(cluster, shard, dc);
        List<Sentinel> sentinels = sentinelGroupModel.getSentinels().stream().map(sentinelInstanceModel -> new Sentinel(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()), sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort())).collect(Collectors.toList());
        sentinels.forEach(sentinel -> {
            sentinelManager.removeSentinelMonitor(sentinel, sentinelMonitorName);
        });
    }

    //6、支持修改分片上的哨兵信息：
    @RequestMapping(value = "/shard/sentinels", method = RequestMethod.POST)
    public RetMessage updateDcShardSentinels(@RequestParam String dc, @RequestParam String cluster, @RequestParam String shard) {
        logger.info("[updateDcShardSentinels] begin to remove {}:{} sentinels", dc, shard);
        try {
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.findAllMeta(dc, cluster, shard);
            if (dcClusterShardTbl == null)
                return RetMessage.createFailMessage(String.format("%s:%s:%s not found", dc, cluster, shard));

            return updateShardSentinels(dcClusterShardTbl);
        } catch (Exception e) {
            logger.error("[updateDcShardSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/redis/sentinels", method = RequestMethod.POST)
    public RetMessage updateRedisSentinels(@RequestParam String ip, @RequestParam int port) {
        logger.info("[updateRedisSentinels] begin to update {}:{} sentinels", ip, port);
        try {
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.findAllByRedis(ip, port);
            if (dcClusterShardTbl == null)
                return RetMessage.createFailMessage(String.format("dc cluster shard not found by redis %s:%d", ip, port));
            
            return updateShardSentinels(dcClusterShardTbl);
        } catch (Exception e) {
            logger.error("[updateRedisSentinels]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }
    
    private RetMessage updateShardSentinels(DcClusterShardTbl dcClusterShardTbl) throws DalException {
        String clusterType = dcClusterShardTbl.getClusterInfo().getClusterType();
        String clusterName = dcClusterShardTbl.getClusterInfo().getClusterName();
        String shardName = dcClusterShardTbl.getShardInfo().getShardName();
        String dcName = dcClusterShardTbl.getDcInfo().getDcName();
        
        SentinelGroupModel selected = sentinelBalanceService.selectSentinel(dcName, ClusterType.lookup(clusterType));
        if (dcClusterShardTbl.getSetinelId() == selected.getSentinelGroupId())
            return RetMessage.createSuccessMessage("current sentinel is suitable, no change");
        
        List<DcClusterShardTbl> dcClusterShardTbls = Lists.newArrayList(dcClusterShardTbl);
        if (clusterType.equalsIgnoreCase(ClusterType.CROSS_DC.name())) {
            dcName = consoleConfig.crossDcSentinelMonitorNameSuffix();
            dcClusterShardTbls.addAll(dcClusterShardService.find(clusterName, shardName));
        }

        for (DcClusterShardTbl dcClusterShard : dcClusterShardTbls) {
            dcClusterShardService.updateDcClusterShard(dcClusterShard.setSetinelId(selected.getSentinelGroupId()));
        }

        String sentinelMonitorName = SentinelUtil.getSentinelMonitorName(clusterName, shardName, dcName);
        SentinelGroupModel current = sentinelGroupService.findById(dcClusterShardTbl.getSetinelId());

        if (current != null) {
            List<Sentinel> currentSentinels = current.getSentinels().stream().map(sentinelInstanceModel -> new Sentinel(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()), sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort())).collect(Collectors.toList());
            currentSentinels.forEach(sentinel -> {
                sentinelManager.removeSentinelMonitor(sentinel, sentinelMonitorName);
            });
        }

        List<RedisTbl> redisTbls =new ArrayList<>();
        for (DcClusterShardTbl dcClusterShard : dcClusterShardTbls)
            redisService.findAllByDcClusterShard(dcClusterShard.getDcClusterShardId());
        
        HostPort master = null;
        for (RedisTbl redis : redisTbls) {
            if (redis.isMaster()) {
                master = new HostPort(redis.getRedisIp(), redis.getRedisPort());
                break;
            }
        }
        List<Sentinel> selectedSentinels = selected.getSentinels().stream().map(sentinelInstanceModel -> new Sentinel(String.format("%s:%d", sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort()), sentinelInstanceModel.getSentinelIp(), sentinelInstanceModel.getSentinelPort())).collect(Collectors.toList());
        for (Sentinel sentinel : selectedSentinels) {
            sentinelManager.monitorMaster(sentinel, sentinelMonitorName, master, consoleConfig.getQuorum());
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
