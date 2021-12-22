package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.redis.core.spring.AbstractWebConfig;
import org.apache.velocity.app.VelocityEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * @author lishanglin
 * date 2021/5/18
 */
@Configuration
public class ConsoleWebConfig extends AbstractWebConfig {

    @Bean
    public VelocityEngine getVelocityEngine() {
        Properties props = new Properties();
        props.put("resource.loader", "class");
        props.put("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        return new VelocityEngine(props);
    }

}
