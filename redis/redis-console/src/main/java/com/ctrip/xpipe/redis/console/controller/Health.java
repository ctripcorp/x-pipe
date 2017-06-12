package com.ctrip.xpipe.redis.console.controller;


import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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


    @RequestMapping(value = "/health", method = RequestMethod.GET)
    public Map<String, Object> getHealthState(HttpServletResponse response) throws Exception {

        if (!consoleLeaderElector.amILeader()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }

        return getHealthState();
    }

    public Map<String, Object> getHealthState() throws Exception {

        Map<String, Object> result = new HashMap<>();
        result.put("isLeader", consoleLeaderElector.amILeader());
        result.put("status", consoleLeaderElector.getAllServers());
        return result;
    }
}
