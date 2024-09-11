package com.ctrip.xpipe.redis.console.controller.api.checker;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.checker.CheckerManager;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.service.CrossMasterDelayService;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.*;

/**
 * @author lishanglin
 * date 2021/3/16
 */
@RestController
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE)
public class ConsoleCheckerController extends AbstractConsoleController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CheckerManager checkerManager;

    @Autowired
    private ProxyManager proxyManager;

    @Autowired
    private DelayService delayService;

    @Autowired
    private PingService pingService;

    @Autowired
    private ClusterHealthMonitorManager clusterHealthMonitorManager;

    @Autowired
    private CrossMasterDelayService crossMasterDelayService;

    @Autowired
    private HealthStateService healthStateService;

    @Autowired
    private OuterClientCache outerClientCache;

    @Autowired
    private KeeperContainerUsedInfoAnalyzer keeperContainerUsedInfoAnalyzer;

    @Autowired
    private CheckerDbConfig checkerDbConfig;

    private Logger logger = LoggerFactory.getLogger(ConsoleCheckerController.class);

    @GetMapping(ConsoleCheckerPath.PATH_GET_META)
    public String getDividedMeta(@PathVariable int index, @RequestParam(value="format", required = false) String format) {
        if (index < 0) throw new IllegalArgumentException("illegal index " + index);
        if (format != null && format.equals("xml")) return metaCache.getXmlFormatDividedXpipeMeta(index);
        return coder.encode(metaCache.getDividedXpipeMeta(index));
    }
    
    @GetMapping(ConsoleCheckerPath.PATH_GET_ALL_META)
    public String getDividedMeta(@RequestParam(value="format", required = false) String format) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        return (format != null && format.equals("xml"))? xpipeMeta.toString() : coder.encode(xpipeMeta);
    }

    @GetMapping(ConsoleCheckerPath.PATH_GET_DC_ALL_META)
    public String getDcAllMeta(@PathVariable String dcName, @RequestParam(value="format", required = false) String format) {
        DcMeta dcMeta = metaCache.getXpipeMeta().getDcs().get(dcName);
        XpipeMeta xpipeMeta = new XpipeMeta().addDc(dcMeta);
        return (format != null && format.equals("xml"))? xpipeMeta.toString() : coder.encode(xpipeMeta);
    }

    @GetMapping(ConsoleCheckerPath.PATH_GET_PROXY_CHAINS)
    public List<ProxyTunnelInfo> getProxyTunnels() {
        return proxyManager.getAllProxyTunnels();
    }

    @PutMapping(ConsoleCheckerPath.PATH_PUT_CHECKER_STATUS)
    public void updateCheckerStatus(@RequestBody CheckerStatus checkerStatus) {
        if (null == checkerStatus.getHostPort()) throw new IllegalArgumentException("checker hostport required");
        if (checkerStatus.getPartIndex() < 0) throw new IllegalArgumentException("illegal index " + checkerStatus.getPartIndex());

        checkerManager.refreshCheckerStatus(checkerStatus);
    }

    @PutMapping(ConsoleCheckerPath.PATH_PUT_HEALTH_CHECK_RESULT)
    public void reportHealthCheckResult(HttpServletRequest request, @RequestBody HealthCheckResult checkResult) {
        logger.debug("[reportHealthCheckResult][{}] {}", request.getRemoteAddr(), checkResult);
        if (null != checkResult.getRedisDelays()) delayService.updateRedisDelays(checkResult.decodeRedisDelays());
        if (null != checkResult.getCrossMasterDelays()) crossMasterDelayService.updateCrossMasterDelays(checkResult.decodeCrossMasterDelays());
        if (null != checkResult.getWarningClusterShards()) clusterHealthMonitorManager.updateHealthCheckWarningShards(checkResult.getWarningClusterShards());
        if (null != checkResult.getRedisAlives()) pingService.updateRedisAlives(checkResult.decodeRedisAlives());
        if (null != checkResult.getRedisStates()) healthStateService.updateHealthState(checkResult.decodeRedisStates());
        if (null != checkResult.getHeteroShardsDelay()) delayService.updateHeteroShardsDelays(checkResult.getHeteroShardsDelay());
    }

    @PostMapping(ConsoleCheckerPath.PATH_POST_KEEPER_CONTAINER_INFO_RESULT)
    public void updateKeeperContainerUsedInfo(HttpServletRequest request, @PathVariable int index, @RequestBody List<KeeperContainerUsedInfoModel> keeperContainerUsedInfoModels) {
        logger.debug("[updateKeeperContainerUsedInfo][{}] {}", request.getRemoteAddr(), keeperContainerUsedInfoModels);
        if (checkerDbConfig.isKeeperBalanceInfoCollectOn()) {
            keeperContainerUsedInfoAnalyzer.updateKeeperContainerUsedInfo(index, keeperContainerUsedInfoModels);
        }
    }


    @Resource
    PersistenceCache persistenceCache;
    
    @RequestMapping(value = ConsoleCheckerPath.PATH_GET_IS_CLUSTER_ON_MIGRATION, method = RequestMethod.GET)
    public boolean isClusterOnMigration(@PathVariable String clusterName) {
        return persistenceCache.isClusterOnMigration(clusterName);
    }

    ObjectMapper objectMapper = new ObjectMapper();
    @RequestMapping(value = ConsoleCheckerPath.PATH_PUT_UPDATE_REDIS_ROLE, method = RequestMethod.PUT)
    public RetMessage updateRedisRole(@PathVariable String role, @RequestBody String body) {
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        DefaultRedisInstanceInfo info;
        try {
            info = objectMapper.readValue(body, DefaultRedisInstanceInfo.class);
        }catch (Exception e) {
            logger.error("[updateRedisRole] parse json error: {}", body);
            return RetMessage.createFailMessage(e.getMessage());
        }
        instance.setInstanceInfo(info);
        persistenceCache.updateRedisRole(instance, Server.SERVER_ROLE.of(role));
        return RetMessage.createSuccessMessage(RetMessage.SUCCESS);
    }

    @RequestMapping(value = ConsoleCheckerPath.PATH_GET_SENTINEL_CHECKER_WHITE_LIST, method = RequestMethod.GET)
    public Set<String> sentinelCheckWhiteList() {
        return persistenceCache.sentinelCheckWhiteList();
    }

    @RequestMapping(value = ConsoleCheckerPath.PATH_GET_CLUSTER_ALERT_WHITE_LIST, method = RequestMethod.GET)
    public Set<String> clusterAlertWhiteList() {
        return persistenceCache.clusterAlertWhiteList();
    }

    @GetMapping(ConsoleCheckerPath.PATH_GET_MIGRATING_CLUSTER_LIST)
    public Set<String> migratingClusterList() {
        return persistenceCache.migratingClusterList();
    }

    @RequestMapping(value = ConsoleCheckerPath.PATH_GET_IS_SENTINEL_AUTO_PROCESS, method = RequestMethod.GET)
    public boolean isSentinelAutoProcess() {
        return persistenceCache.isSentinelAutoProcess();
    }
    @RequestMapping(value = ConsoleCheckerPath.PATH_GET_IS_ALERT_SYSTEM_ON, method = RequestMethod.GET)
    public boolean isAlertSystemOn() {
        return persistenceCache.isAlertSystemOn();
    }

    @RequestMapping(value = ConsoleCheckerPath.PATH_GET_IS_KEEPER_BALANCE_INFO_COLLECT_ON, method = RequestMethod.GET)
    public boolean isKeeperBalanceInfoCollectOn() {
        return persistenceCache.isKeeperBalanceInfoCollectOn();
    }

    @RequestMapping(value = ConsoleCheckerPath.PATH_GET_CLUSTER_CREATE_TIME, method = RequestMethod.GET)
    public Date getClusterCreateTime(@PathVariable String clusterId) {
        return persistenceCache.getClusterCreateTime(clusterId);
    }

    @RequestMapping(value = ConsoleCheckerPath.PATH_GET_LOAD_ALL_CLUSTER_CREATE_TIME, method = RequestMethod.GET)
    public Map<String, Date> loadAllClusterCreateTime() {
        return persistenceCache.loadAllClusterCreateTime();
    }

    @Autowired
    AlertEventService alertEventService;

    @RequestMapping(value = ConsoleCheckerPath.PATH_POST_RECORD_ALERT, method = RequestMethod.POST)
    public void recordAlert(@RequestBody CheckerConsoleService.AlertMessage alertMessage) {
        this.persistenceCache.recordAlert(alertMessage.getEventOperator(), alertMessage.getMessage(), alertMessage.getEmailResponse());
    }

    @GetMapping(value = ConsoleCheckerPath.PATH_GET_ALL_CURRENT_DC_ACTIVE_DC_ONE_WAY_CLUSTERS)
    public Map<String, OuterClientService.ClusterInfo> loadAllOuterClientClusters(@RequestParam String activeDc) {
        return outerClientCache.getAllActiveDcClusters(activeDc);
    }


}
