package com.ctrip.xpipe.redis.proxy.spring;

import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */

@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile {

    @Bean
    public ProxyEndpointManager getProxyEndpointManager() {
        return new DefaultProxyEndpointManager();
    }
}
