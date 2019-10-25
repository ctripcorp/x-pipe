package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.GenericRetMessage;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.SentinelModel;
import com.ctrip.xpipe.redis.console.model.SentinelUsageModel;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
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
    private ClusterService clusterService;

    @Autowired
    private SentinelService sentinelService;

    @Autowired
    private DcService dcService;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final int DEFAULT_NUM_OF_CLUSTERS = 10;

    private static final JsonCodec jsonTool = new JsonCodec(true, true);

    @RequestMapping(value = "/rebalance/sentinels/{numOfClusters}", method = RequestMethod.POST)
    public RetMessage reBalanceSentinels(@PathVariable int numOfClusters) {
        logger.info("[reBalanceSentinels] Start re-balance sentinels for {} clusters", numOfClusters);
        try {
            List<String> modifiedClusters = clusterService.reBalanceSentinels(numOfClusters);
            logger.info("[reBalanceSentinels] Successfully balanced {} clusters", numOfClusters);
            return GenericRetMessage.createGenericRetMessage(modifiedClusters);
        } catch (Exception e) {
            logger.error("[reBalanceSentinels] {}", e);
            return RetMessage.createFailMessage(e.getMessage());
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

    @RequestMapping(value = "/rebalance/sentinels", method = RequestMethod.POST)
    public RetMessage reBalanceSentinels(@RequestBody(required = false) List<String> clusterNames) {
        if(clusterNames == null || clusterNames.isEmpty())
            return reBalanceSentinels(DEFAULT_NUM_OF_CLUSTERS);
        logger.info("[reBalanceSentinels] Start re-balance clusters: {}", clusterNames);
        try {
            clusterService.reBalanceClusterSentinels(clusterNames);
            logger.info("[reBalanceSentinels] Successfully balanced clusters: {}", clusterNames);
            return RetMessage.createSuccessMessage("clusters: " + jsonTool.encode(clusterNames));
        } catch (Exception e) {
            logger.error("[reBalanceSentinels] {}", e);
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
            logger.error("[reBalanceSentinels] {}", e);
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
