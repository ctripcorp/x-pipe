package com.ctrip.framework.xpipe.redis.servlet.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;

@Configuration
public class AutoConfiguration {

    @Configuration
    @Conditional(NoneFilterRegistrationCondition.class)
    static class ConfigurationNew {

        @Autowired
        Environment environment;

        @Bean(name = "ProxyFilterRegistrationBeanNew")
        public org.springframework.boot.web.servlet.FilterRegistrationBean factory() throws ServletException {
            org.springframework.boot.web.servlet.FilterRegistrationBean filter =
                    new org.springframework.boot.web.servlet.FilterRegistrationBean();
            initFilter(filter);
            return filter;
        }
    }

    @Configuration
    @Conditional(FilterRegistrationCondition.class)
    static class ConfigurationOld {

        @Autowired
        Environment environment;

        @Bean(name = "ProxyFilterRegistrationBeanOld")
        public org.springframework.boot.context.embedded.FilterRegistrationBean factory() throws ServletException {
            org.springframework.boot.context.embedded.FilterRegistrationBean filter =
                    new org.springframework.boot.context.embedded.FilterRegistrationBean();
            initFilter(filter);
            return filter;
        }
    }

    private static void initFilter(FilterRegistrationBean filter) throws ServletException {
        filter.setFilter(new ProxyFilter());
        filter.setName("proxy-filter");
        filter.addUrlPatterns("/proxy/client");
        filter.setDispatcherTypes(DispatcherType.REQUEST, DispatcherType.FORWARD);
        filter.setAsyncSupported(true);
        filter.setOrder(Ordered.HIGHEST_PRECEDENCE);
    }

}
