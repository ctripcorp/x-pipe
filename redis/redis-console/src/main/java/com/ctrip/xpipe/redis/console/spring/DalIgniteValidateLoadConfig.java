package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(false)
@ComponentScan(basePackages = {"com.ctrip.xpipe.service.ignite"})
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE)
public class DalIgniteValidateLoadConfig {
    // only config not set disable db, then laod com.ctrip.xpipe.service.ignite.DalIgniteValidate
}
