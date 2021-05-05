package com.ctrip.xpipe.redis.console.controller.api.checker;

import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.console.checker.CheckerManager;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.service.CrossMasterDelayService;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

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

    private Logger logger = LoggerFactory.getLogger(ConsoleCheckerController.class);

    @GetMapping(ConsoleCheckerPath.PATH_GET_META)
    public String getDividedMeta(@PathVariable int index, @RequestParam(value="format", required = false) String format) {
        if (index < 0) throw new IllegalArgumentException("illegal index " + index);
        XpipeMeta xpipeMeta = metaCache.getDividedXpipeMeta(index);

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
    }

}
