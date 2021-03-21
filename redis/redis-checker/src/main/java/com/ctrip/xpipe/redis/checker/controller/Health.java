package com.ctrip.xpipe.redis.checker.controller;

import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lishanglin
 * date 2021/3/21
 */
@RestController
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CHECKER)
public class Health {

    @RequestMapping(value = "/health", method = RequestMethod.GET)
    public boolean health() {
        return true;
    }

}
