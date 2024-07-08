package com.ctrip.xpipe.redis.console.controller;

import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lishanglin
 * date 2024/7/8
 */
@RestController
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
@RequestMapping("/api/debug")
public class DebugApi extends AbstractConsoleController {

    @Autowired
    private ConsoleLeaderElector consoleLeaderElector;

    @GetMapping(value = "/leader/reset")
    public void forceLeaderReset() {
        this.consoleLeaderElector.forceReset();
    }

    @GetMapping(value = "/leader/force")
    public void forceLeader(@RequestParam(required = false, defaultValue = "true") boolean leader) {
        if (leader) {
            consoleLeaderElector.forceSetLeader();
        } else {
            consoleLeaderElector.forceSetFollower();
        }
    }

}
