package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.service.impl.DefaultCrossMasterDelayService;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.console.util.HickwallMetricInfo;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @author shyin
 *         <p>
 *         Jan 5, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class HealthCheckController extends AbstractConsoleController {

    @Autowired
    private PingService pingService;
    @Autowired
    private DelayService delayService;
    @Autowired
    private ConsoleConfig config;

    @Autowired
    private DefaultCrossMasterDelayService crossMasterDelayService;

    private static final String INSTANCE_DELAY_TEMPLATE = "&panelId=%d&var-cluster=%s&var-shard=%s&var-address=%s:%d";
    private static final String CROSS_DC_DELAY_TEMPLATE = "&panelId=%d&var-cluster=%s&var-shard=%s&var-source=%s&var-dest=%s";

    private static final String ENDCODE_TYPE = "UTF-8";

    @RequestMapping(value = "/redis/health/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Boolean> isRedisHealth(@PathVariable String redisIp, @PathVariable int redisPort) {
        return ImmutableMap.of("isHealth", pingService.isRedisAlive(new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/redis/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Long> getReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
        return ImmutableMap.of("delay", delayService.getDelay(new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/redis/delay/{clusterType}/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Long> getReplDelayMillis(@PathVariable String clusterType, @PathVariable String redisIp, @PathVariable int redisPort) {
        ClusterType type = ClusterType.lookup(clusterType);
        return ImmutableMap.of("delay", delayService.getDelay(type, new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/cross-master/delay/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
    public Map<String, Pair<HostPort, Long>> getCrossMasterReplHealthStatus(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId) {
        return crossMasterDelayService.getPeerMasterDelayFromSourceDc(dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/cross-master/delay/{clusterType}/{dcId}/" + CLUSTER_ID_PATH_VARIABLE + "/" + SHARD_ID_PATH_VARIABLE, method = RequestMethod.GET)
    public Map<String, Pair<HostPort, Long>> getCrossMasterReplHealthStatus(@PathVariable String clusterType, @PathVariable String dcId,
                                                                            @PathVariable String clusterId, @PathVariable String shardId) {
        ClusterType type = ClusterType.lookup(clusterType);
        return crossMasterDelayService.getPeerMasterDelayFromSourceDc(type, dcId, clusterId, shardId);
    }

    @RequestMapping(value = "/redis/health/hickwall/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE + "/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, String> getHickwallAddress(@PathVariable String clusterName, @PathVariable String shardName, @PathVariable String redisIp, @PathVariable int redisPort) {
        HickwallMetricInfo info = config.getHickwallMetricInfo();
        if (Strings.isEmpty(info.getDomain())) {
            return ImmutableMap.of("addr", "");
        }
        String template = null;
        try {
            template = String.format(INSTANCE_DELAY_TEMPLATE, info.getDelayPanelId(), clusterName, shardName, redisIp, redisPort);
        } catch (Exception e) {
            logger.error("[getHickwallAddress]", e);
            return ImmutableMap.of("addr", "");
        }
        String url = info.getDomain() + template;
        return ImmutableMap.of("addr", url);
    }
    
    @RequestMapping(value = "/cluster/health/hickwall/" + CLUSTER_TYPE_VARIABLE + "/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET) 
    public Map<String, String> getClusterHickwallAddress(@PathVariable String clusterType, @PathVariable String clusterName) {
        HickwallMetricInfo info = config.getHickwallMetricInfo();
        ClusterType clusterType1 = ClusterType.lookup(clusterType);
        String template = "";
        if (clusterType1.supportMultiActiveDC()) {
            template = info.getBiDirectionClusterTemplateUrl();
        } else {
            template = info.getOneWayClusterTemplateUrl();
        }
        if (Strings.isEmpty(template)) {
            return ImmutableMap.of("addr", "");
        }
        String url =  String.format(template, clusterName);
        return ImmutableMap.of("addr", url);
    }
    

    @RequestMapping(value = "/cross-master/health/hickwall/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE + "/{sourceDc}/{destDc}", method = RequestMethod.GET)
    public Map<String, String> getCrossDcDelayHickwallAddress(@PathVariable String clusterName, @PathVariable String shardName, @PathVariable String sourceDc, @PathVariable String destDc) {
        HickwallMetricInfo info = config.getHickwallMetricInfo();
        if (Strings.isEmpty(info.getDomain())) {
            return ImmutableMap.of("addr", "");
        }
        String template;
        try {
            template = String.format(CROSS_DC_DELAY_TEMPLATE, info.getCrossDcDelayPanelId(), clusterName, shardName, sourceDc, destDc);
        } catch (Exception e) {
            logger.error("[getCrossDcDelayHickwallAddress]", e);
            return ImmutableMap.of("addr", "");
        }
        String url = info.getDomain() + template;
        return ImmutableMap.of("addr", url);
    }
}
