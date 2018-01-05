package com.ctrip.xpipe.redis.console.controller.api.data;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.SentinelModel;
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

    @RequestMapping(value = "/reBalance/sentinels/{numOfClusters}", method = RequestMethod.POST)
    public RetMessage reBalanceSentinels(@PathVariable int numOfClusters) {
        logger.info("[reBalanceSentinels] Start re-balance sentinels for {} clusters", numOfClusters);
        try {
            List<String> modifiedClusters = clusterService.reBalanceSentinels(numOfClusters);
            logger.info("[reBalanceSentinels] Successfully balanced {} clusters", numOfClusters);
            return RetMessage.createSuccessMessage("clusters: " + JSON.toJSONString(modifiedClusters));
        } catch (Exception e) {
            logger.error("[reBalanceSentinels] {}", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/reBalance/sentinels", method = RequestMethod.POST)
    public RetMessage reBalanceSentinels() {
        return reBalanceSentinels(DEFAULT_NUM_OF_CLUSTERS);
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
