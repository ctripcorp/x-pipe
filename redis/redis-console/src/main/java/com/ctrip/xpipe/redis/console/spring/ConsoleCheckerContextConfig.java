package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.console.spring.condition.ConsoleServerMode;
import com.ctrip.xpipe.redis.console.spring.condition.ConsoleServerModeCondition;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * @author lishanglin
 * date 2021/3/13
 */
@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = {"com.ctrip.xpipe.service.sso", "com.ctrip.xpipe.redis.console", "com.ctrip.xpipe.redis.checker"})
@ServletComponentScan("com.ctrip.framework.fireman")
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE_CHECKER)
public class ConsoleCheckerContextConfig extends ConsoleContextConfig {

}
