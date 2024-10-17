package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.console.service.impl.DefaultCrossMasterDelayService;
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
import java.util.function.Function;

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

    private static final String INSTANCE_DELAY_TEMPLATE = "&panelId=%d&var-cluster=%s&var-shard=%s&var-address=%s:%d&var-delayType=%s";
    private static final String HETERO_DELAY_TEMPLATE = "&panelId=%d&var-cluster=%s&var-srcShardId=%d&var-delayType=%s";
    private static final String CROSS_DC_DELAY_TEMPLATE = "&panelId=%d&var-cluster=%s&var-shard=%s&var-source=%s&var-dest=%s";
    private static final String OUTCOMING_TRAFFIC_TO_PEER_TEMPLATE = "&panelId=%d&var-address=%s:%d";
    private static final String INCOMING_TRAFFIC_FROM_PEER_TEMPLATE = "&panelId=%d&var-address=%s:%d";
    private static final String PEER_SYNC_FULL_TEMPLATE = "&panelId=%d&var-address=%s:%d";
    private static final String PEER_SYNC_PARTIAL_TEMPLATE = "&panelId=%d&var-address=%s:%d";
    
    private static final String ENDCODE_TYPE = "UTF-8";

    @RequestMapping(value = "/shard/delay/{clusterId}/{shardId}/{shardDbId}", method = RequestMethod.GET)
    public Map<String, Long> getShardDelayMillis(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable Long shardDbId) {
        return ImmutableMap.of("delay", delayService.getShardDelay(clusterId, shardId, shardDbId));
    }

    @RequestMapping(value = "/redis/health/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Boolean> isRedisHealth(@PathVariable String redisIp, @PathVariable int redisPort) {
        return ImmutableMap.of("isHealth", pingService.isRedisAlive(new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/redis/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Long> getReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
        return ImmutableMap.of("delay", delayService.getDelay(new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/redises/delay/{dcId}/{clusterId}", method = RequestMethod.GET)
    public Map<String, Map<HostPort, Long>> getAllReplDelayMillis(@PathVariable String dcId, @PathVariable String clusterId) {
        return ImmutableMap.of("delay", delayService.getDelay(dcId, clusterId));
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

    String getHickwallUrl(Function<HickwallMetricInfo, String> function) throws Exception {
        HickwallMetricInfo info = config.getHickwallMetricInfo();
        if (Strings.isEmpty(info.getDomain())) {
            return "";
        }
        String template = function.apply(info);
        String url = info.getDomain() + template;
        return url;
    }
    
    @RequestMapping(value = "/redis/health/hickwall/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE + "/{redisIp}/{redisPort}/{delayType}", method = RequestMethod.GET)
    public Map<String, String> getHickwallAddress(@PathVariable String clusterName, @PathVariable String shardName, @PathVariable String redisIp, @PathVariable int redisPort, @PathVariable String delayType) {
        String url = "";
        try {
             url = getHickwallUrl(info -> String.format(INSTANCE_DELAY_TEMPLATE, info.getDelayPanelId(), clusterName, shardName, redisIp, redisPort, delayType));
        } catch (Exception e) {
            logger.error("[getHickwallUrl]", e);
        }
        return ImmutableMap.of("addr", url);
    }

    @RequestMapping(value = "/hetero/health/hickwall/" + CLUSTER_NAME_PATH_VARIABLE + "/{srcShardId}/{delayType}", method = RequestMethod.GET)
    public Map<String, String> getHeteroDelayHickwallAddress(@PathVariable String clusterName, @PathVariable int srcShardId, @PathVariable String delayType) {
        String url = "";
        try {
            url = getHickwallUrl(info -> String.format(HETERO_DELAY_TEMPLATE, info.getHeteroDelayPanelId(), clusterName, srcShardId, delayType));
        } catch (Exception e) {
            logger.error("[getHeteroDelayHickwallAddress]", e);
        }
        return ImmutableMap.of("addr", url);
    }

    @RequestMapping(value = "/cross-master/health/hickwall/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE + "/{sourceDc}/{destDc}", method = RequestMethod.GET)
    public Map<String, String> getCrossDcDelayHickwallAddress(@PathVariable String clusterName, @PathVariable String shardName, @PathVariable String sourceDc, @PathVariable String destDc) {
        String url = "";
        try {
            url = getHickwallUrl(info -> String.format(CROSS_DC_DELAY_TEMPLATE, info.getCrossDcDelayPanelId(), clusterName, shardName, sourceDc, destDc));
        } catch (Exception e) {
            logger.error("[getHickwallUrl]", e);
        }
        return ImmutableMap.of("addr", url);
    }

    @RequestMapping(value = "/redis/outcoming/traffic/to/peer/hickwall/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, String> getOutComingTrafficToPeerHickwallAddress(@PathVariable String redisIp, @PathVariable int redisPort) {
        String url = "";
        try {
            url = getHickwallUrl(info -> String.format(OUTCOMING_TRAFFIC_TO_PEER_TEMPLATE, info.getOutComingTrafficToPeerPanelId(), redisIp, redisPort));
        } catch (Exception e) {
            logger.error("[getHickwallUrl]", e);
        }
        return ImmutableMap.of("addr", url);
    }
    
    @RequestMapping(value = "/redis/incoming/traffic/from/peer/hickwall/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, String> getInComingTrafficFromPeerHickwallAddress(@PathVariable String redisIp, @PathVariable int redisPort) {
        String url = "";
        try {
            url = getHickwallUrl(info -> String.format(INCOMING_TRAFFIC_FROM_PEER_TEMPLATE, info.getInComingTrafficFromPeerPanelId(), redisIp, redisPort));
        } catch (Exception e) {
            logger.error("[getHickwallUrl]", e);
        }
        return ImmutableMap.of("addr", url);
    }

    @RequestMapping(value = "/redis/peer/sync/full/hickwall/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, String> getPeerSyncFullHickwallAddress(@PathVariable String redisIp, @PathVariable int redisPort) {
        String url = "";
        try {
            url = getHickwallUrl(info -> String.format(PEER_SYNC_FULL_TEMPLATE, info.getPeerSyncFullPanelId(), redisIp, redisPort));
        } catch (Exception e) {
            logger.error("[getHickwallUrl]", e);
        }
        return ImmutableMap.of("addr", url);
    }

    @RequestMapping(value = "/redis/peer/sync/partial/hickwall/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, String> getPeerSyncPartialHickwallAddress(@PathVariable String redisIp, @PathVariable int redisPort) {
        String url = "";
        try {
            url = getHickwallUrl(info -> String.format(PEER_SYNC_PARTIAL_TEMPLATE, info.getPeerSyncPartialPanelId(), redisIp, redisPort));
        } catch (Exception e) {
            logger.error("[getHickwallUrl]", e);
        }
        return ImmutableMap.of("addr", url);
    }
}
