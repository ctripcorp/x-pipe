package com.ctrip.xpipe.testutils.stateful.config;

import com.ctrip.xpipe.testutils.stateful.StateHolder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Slight
 * <p>
 * Sep 17, 2021 10:45 PM
 */
@Configuration
public class StatefulConfig {

    @Bean
    public StateHolder stateHolder() {
        return new StateHolder(System.getProperty("server.state"));
    }
}
