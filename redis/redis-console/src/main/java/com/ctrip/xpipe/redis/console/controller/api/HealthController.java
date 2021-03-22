package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.service.impl.DefaultCrossMasterDelayService;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
@ConditionalOnProperty(name = { HealthChecker.ENABLED }, matchIfMissing = true)
public class HealthController extends AbstractConsoleController{

    @Autowired
    private DelayService delayService;

    @Autowired
    private DefaultCrossMasterDelayService crossMasterDelayService;

    @RequestMapping(value = "/redis/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Long getReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
        return delayService.getDelay(new HostPort(redisIp, redisPort));
    }


    @RequestMapping(value = "/redis/inner/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Long getInnerReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
        return delayService.getLocalCachedDelay(new HostPort(redisIp, redisPort));
    }

    @RequestMapping(value = "/redis/inner/delay/all", method = RequestMethod.GET)
    public Map<HostPort, Long> getAllInnerReplDelayMills() {
        return delayService.getDcCachedDelay(FoundationService.DEFAULT.getDataCenter());
    }

    @RequestMapping(value = "/redis/inner/unhealthy", method = RequestMethod.GET)
    public UnhealthyInfoModel getActiveClusterUnhealthyRedis() {
        return delayService.getDcActiveClusterUnhealthyInstance(FoundationService.DEFAULT.getDataCenter());
    }

    @RequestMapping(value = "/redis/inner/unhealthy/all", method = RequestMethod.GET)
    public UnhealthyInfoModel getAllUnhealthyRedis() {
        return delayService.getAllUnhealthyInstance();
    }

    @RequestMapping(value = "/cross-master/delay/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
    public Map<String, Pair<HostPort, Long>> getCrossMasterDelay(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return crossMasterDelayService.getPeerMasterDelayFromSourceDc(dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/cross-master/inner/delay/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
    public Map<String, Pair<HostPort, Long>> getInnerCrossMasterDelay(@PathVariable String clusterId, @PathVariable String shardId) {
        return crossMasterDelayService.getPeerMasterDelayFromCurrentDc(clusterId, shardId);
    }
}
