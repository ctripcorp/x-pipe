package com.ctrip.xpipe.redis.console.controller;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.checker.CheckerManager;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.IpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
@RestController
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Health extends AbstractConsoleController {

    @Autowired
    private ConsoleLeaderElector consoleLeaderElector;

    @Autowired
    private CrossDcClusterServer crossDcClusterServer;

    @Autowired(required = false)
    private CheckerManager checkerManager;

    @Autowired
    private ConsoleConfig config;

    @RequestMapping(value = "/health", method = RequestMethod.GET)
    public Map<String, Object> getHealthState(HttpServletRequest request, HttpServletResponse response) {

        if (!consoleLeaderElector.amILeader() && !isFromLocal(request)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }

        return getHealthState();
    }

    protected boolean isFromLocal(HttpServletRequest request){

        logger.debug("[isFromLocal]{}", request.getRemoteHost());
        return IpUtils.isLocal(request.getRemoteHost());
    }

    public Map<String, Object> getHealthState() {

        Map<String, Object> result = new HashMap<>();
        result.put("isLeader", consoleLeaderElector.amILeader());
        result.put("status", consoleLeaderElector.getAllServers());
        result.put("crossDcLeader", crossDcClusterServer.amILeader());
        result.put("dc", FoundationService.DEFAULT.getDataCenter());
        result.put("type", config.getOwnClusterType());
        result.put("rawtype", System.getProperty("console.cluster.types", ""));

        if (null != checkerManager) {
            result.put("checker", checkerManager.getCheckers());
        }

        return result;
    }
}
