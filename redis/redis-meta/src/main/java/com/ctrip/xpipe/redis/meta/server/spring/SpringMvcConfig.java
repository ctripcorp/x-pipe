package com.ctrip.xpipe.redis.meta.server.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.spring.DomainValidateHandlerInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.util.function.Supplier;

@Configuration
@ComponentScan(basePackages = {"com.ctrip.xpipe.redis.meta.server"})
public class SpringMvcConfig  extends WebMvcConfigurerAdapter {

    @Autowired
    private MetaServerConfig metaServerConfig;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        Supplier<String> expectedDomainName = () -> {
            // toLowerCase() to match metaServerConfig retrieve info
            String dcName = FoundationService.DEFAULT.getDataCenter().toLowerCase();
            DcInfo dcInfo = metaServerConfig.getDcInofs().get(dcName);
            return dcInfo.getMetaServerAddress();
        };
        registry.addInterceptor(new DomainValidateHandlerInterceptor(expectedDomainName));
    }

}