package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.checker.spring.FiremanServletScanCondition;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@ServletComponentScan("com.ctrip.framework.fireman")
@Conditional(FiremanServletScanCondition.class)
public class FiremanServletScanConfig {
}
