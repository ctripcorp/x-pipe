package com.ctrip.xpipe.redis.meta.server.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;

/**
 * @author marsqing
 *         <p>
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan(basePackages = {"com.ctrip.xpipe.redis.meta.server"})
public class MetaServerContextConfig extends AbstractRedisConfigContext {

    public static final String CLIENT_POOL = "clientPool";

    @Bean(name = CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getClientPool() {

        return new XpipeNettyClientKeyedObjectPool();
    }


    @Bean
    public MetaServerMultiDcServiceManager getMetaServerMultiDcServiceManager() {

        return new DefaultMetaServerMultiDcServiceManager();
    }
}
