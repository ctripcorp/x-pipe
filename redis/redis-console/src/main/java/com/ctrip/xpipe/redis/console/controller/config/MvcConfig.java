package com.ctrip.xpipe.redis.console.controller.config;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
@Component
public class MvcConfig extends WebMvcConfigurerAdapter{

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ConsoleConfig config;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LogInterceptor());
        registry.addInterceptor(new ClusterCheckInterceptor(metaCache, clusterService));
        if(config.disableDb()) {
            registry.addInterceptor(new DisableConsoleInterceptor());
        }
    }

}
