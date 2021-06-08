package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.DefaultPingService;
import com.ctrip.xpipe.redis.checker.impl.CheckerRedisInfoManager;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DcIgnoredConfigChangeListener;
import com.ctrip.xpipe.redis.console.service.RedisInfoService;
import com.ctrip.xpipe.redis.console.service.impl.DefaultRedisInfoService;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.Bean;
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

    @Bean
    public DcIgnoredConfigChangeListener dcIgnoredConfigChangeListener() {
        return new DcIgnoredConfigChangeListener();
    }

    @Bean
    public DefaultPingService pingService() {
        return new DefaultPingService();
    }

    @Bean
    public CheckerRedisInfoManager redisInfoManager() {
        return new CheckerRedisInfoManager();
    }

    @Bean
    @Override
    public RedisInfoService redisInfoService() {
        return new DefaultRedisInfoService();
    }
}
