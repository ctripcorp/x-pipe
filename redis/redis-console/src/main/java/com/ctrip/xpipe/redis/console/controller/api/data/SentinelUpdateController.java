package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.GenericRetMessage;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

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
    public SentinelBalanceService sentinelBalanceService;

    @Autowired
    private DcService dcService;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final JsonCodec jsonTool = new JsonCodec(true, true);

    @PostMapping("/rebalance/sentinels/{dcName}/execute")
    public RetMessage reBalanceSentinels(@PathVariable String dcName, @RequestParam(defaultValue = "true") boolean backupDcOnly) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        if (backupDcOnly) {
            sentinelBalanceService.rebalanceBackupDcSentinel(dcTbl.getDcName());
        } else {
            sentinelBalanceService.rebalanceDcSentinel(dcTbl.getDcName());
        }
        return RetMessage.createSuccessMessage();
    }

    @PostMapping("/rebalance/sentinels/{dcName}/terminate")
    public RetMessage terminateBalanceSentinels(@PathVariable String dcName) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        sentinelBalanceService.cancelCurrentBalance(dcTbl.getDcName());
        return RetMessage.createSuccessMessage();
    }

    @GetMapping("/rebalance/sentinels/{dcName}/process")
    public RetMessage getSentinelsBalanceProcess(@PathVariable String dcName) {
        DcTbl dcTbl = dcService.findByDcName(dcName);
        if (null == dcTbl) return RetMessage.createFailMessage("unknown dc " + dcName);

        SentinelBalanceTask balanceTask = sentinelBalanceService.getBalanceTask(dcTbl.getDcName());
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
    public RetMessage addSentinel(@RequestBody SentinelModel sentinelModel) {
        try {
            SetinelTbl sentinel = convert2SentinelTbl(sentinelModel);
            sentinelService.insert(sentinel);
            return RetMessage.createSuccessMessage("Successfully create Sentinel");
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinelsV2", method = RequestMethod.POST)
    public RetMessage addSentinelV2(@RequestBody SentinelGroupInfo sentinelGroupInfo) {
        try {
            sentinelService.addSentinelGroup(sentinelGroupInfo);
            return RetMessage.createSuccessMessage("Successfully create Sentinel");
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinels/{sentinelId}", method = RequestMethod.DELETE)
    public RetMessage deleteSentinel(@PathVariable Long sentinelId) {
        try {
            SetinelTbl setinelTbl = sentinelService.find(sentinelId);
            if (setinelTbl == null) {
                return RetMessage.createSuccessMessage("Sentinel already deleted");
            }
            sentinelService.delete(sentinelId);
            return RetMessage.createSuccessMessage("Successfully deleted Sentinel");
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinels/{sentinelId}", method = RequestMethod.PATCH)
    public RetMessage rehealSentinel(@PathVariable Long sentinelId) {
        try {
            sentinelService.reheal(sentinelId);
            return RetMessage.createSuccessMessage("Successfully reheal Sentinel");
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinels/usage", method = RequestMethod.GET)
    public RetMessage sentinelUsage() {
        logger.info("[sentinelUsage] begin to retrieve all sentinels' usage");
        try {
            Map<String, SentinelUsageModel> sentienlUsage = sentinelService.getAllSentinelsUsage();
            return GenericRetMessage.createGenericRetMessage(sentienlUsage);
        } catch (Exception e) {
            logger.error("[sentinelUsage] {}", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinel/address", method = RequestMethod.PUT)
    public RetMessage updateSentinelAddr(@RequestBody SentinelModel model) {
        logger.info("[updateSentinelAddr][begin]");
        try {
            SentinelModel updated = sentinelService.updateSentinelTblAddr(model);
            return RetMessage.createSuccessMessage(jsonTool.encode(updated));
        } catch (Exception e) {
            logger.error("[updateSentinelAddr]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinel/addressV2", method = RequestMethod.PUT)
    public RetMessage updateSentinelAddrV2(@RequestBody SentinelGroupInfo sentinelGroupInfo) {
        logger.info("[updateSentinelAddr][begin]");
        try {
            SentinelGroupInfo updated = sentinelService.updateSentinelGroup(sentinelGroupInfo);
            return RetMessage.createSuccessMessage(jsonTool.encode(updated));
        } catch (Exception e) {
            logger.error("[updateSentinelAddr]", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/sentinel/monitor/{clusterName}", method = RequestMethod.DELETE)
    public RetMessage removeSentinelMonitor(@PathVariable String clusterName) {
        logger.info("[removeSentinelMonitor][begin]");
        try {
            return sentinelService.removeSentinelMonitor(clusterName);
        } catch (Exception e) {
            logger.error("[removeSentinelMonitor]", e);
            return RetMessage.createFailMessage(e.getMessage());
        } finally {
            logger.info("[removeSentinelMonitor][end]");
        }
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
