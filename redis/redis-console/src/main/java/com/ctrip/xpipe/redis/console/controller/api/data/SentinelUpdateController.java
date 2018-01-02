package com.ctrip.xpipe.redis.console.controller.api.data;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.SentinelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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

    private Logger logger = LoggerFactory.getLogger(getClass());

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
        logger.info("[reBalanceSentinels] Re-balance all clusters");
        return reBalanceSentinels(0);
    }
}
