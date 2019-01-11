package com.ctrip.xpipe.redis.meta.server.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.spring.DomainValidateFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.function.Supplier;

/**
 * @author marsqing
 *         <p>
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan(basePackages = {"com.ctrip.xpipe.redis.meta.server"})
public class MetaServerContextConfig extends AbstractRedisConfigContext {

    @Autowired
    private MetaServerConfig metaServerConfig;

    public static final String CLIENT_POOL = "clientPool";

    @Bean(name = CLIENT_POOL)
    public XpipeNettyClientKeyedObjectPool getClientPool() {

        return new XpipeNettyClientKeyedObjectPool();
    }


    @Bean
    public MetaServerMultiDcServiceManager getMetaServerMultiDcServiceManager() {

        return new DefaultMetaServerMultiDcServiceManager();
    }

    @Bean
    public FilterRegistrationBean domainValidateFilter() {
        FilterRegistrationBean registrationBean = new FilterRegistrationBean();
        Supplier<String> expectedDomainName = () -> {
            // toLowerCase() to match metaServerConfig retrieve info
            String dcName = FoundationService.DEFAULT.getDataCenter().toLowerCase();
            DcInfo dcInfo = metaServerConfig.getDcInofs().get(dcName);
            return dcInfo.getMetaServerAddress();
        };
        DomainValidateFilter filter = new DomainValidateFilter(()->metaServerConfig.validateDomain(), expectedDomainName);
        registrationBean.setFilter(filter);
        registrationBean.addUrlPatterns("/*");
        return registrationBean;
    }
}
