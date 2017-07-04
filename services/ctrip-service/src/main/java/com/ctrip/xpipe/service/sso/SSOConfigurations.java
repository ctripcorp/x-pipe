package com.ctrip.xpipe.service.sso;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.ctrip.xpipe.api.config.Config;

import org.springframework.boot.context.embedded.FilterRegistrationBean;
import org.springframework.boot.context.embedded.ServletContextInitializer;
import org.springframework.boot.context.embedded.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.EventListener;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;


/**
 * @author lepdou 2016-11-08
 */
@Configuration
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SSOConfigurations {

    public static final String KEY_CAS_REGISTER_SERVER_NAME = "cas.register.server.name";
    public static final String KEY_CAS_SERVER_LOGIN_URL = "cas.server.login.url";
    public static final String KEY_CAS_SERVER_URL_PREFIX = "cas.server.url.prefix";
    public static final String KEY_CLOGGING_SERVER_URL = "clogging.server.url";
    public static final String KEY_CLOGGING_SERVER_PORT = "clogging.server.port";
    public static final String KEY_CREDIS_SERVER_URL = "credis.server.url";

    private Config config = Config.DEFAULT;

    @Bean
    public ServletListenerRegistrationBean redisAppSettingListener() {
        ServletListenerRegistrationBean redisAppSettingListener = new ServletListenerRegistrationBean();
        redisAppSettingListener.setListener(listener("org.jasig.cas.client.credis.CRedisAppSettingListner"));
        return redisAppSettingListener;
    }

    @Bean
    public ServletListenerRegistrationBean singleSignOutHttpSessionListener() {
        ServletListenerRegistrationBean singleSignOutHttpSessionListener = new ServletListenerRegistrationBean();
        singleSignOutHttpSessionListener
            .setListener(listener("org.jasig.cas.client.session.SingleSignOutHttpSessionListener"));
        return singleSignOutHttpSessionListener;
    }

    @Bean
    public FilterRegistrationBean casFilter() {
        FilterRegistrationBean singleSignOutFilter = new FilterRegistrationBean();
        singleSignOutFilter.setFilter(filter("org.jasig.cas.client.session.SingleSignOutFilter"));
        singleSignOutFilter.addUrlPatterns("/*");
        return singleSignOutFilter;
    }

    @Bean
    public FilterRegistrationBean authenticationFilter() {
        FilterRegistrationBean casFilter = new FilterRegistrationBean();

        Map<String, String> filterInitParam = Maps.newHashMap();
        filterInitParam.put("redisClusterName", "casClientPrincipal");
        filterInitParam.put("serverName", config.get(KEY_CAS_REGISTER_SERVER_NAME));
        filterInitParam.put("casServerLoginUrl", config.get(KEY_CAS_SERVER_LOGIN_URL));
        //we don't want to use session to store login information, since we will be deployed to a cluster, not a single instance
        filterInitParam.put("useSession", "false");

        casFilter.setInitParameters(filterInitParam);
        casFilter.setFilter(new XPipeFilter());
        casFilter.addUrlPatterns("/*");

        return casFilter;
    }

    @Bean
    public FilterRegistrationBean casValidationFilter() {
        FilterRegistrationBean casValidationFilter = new FilterRegistrationBean();
        Map<String, String> filterInitParam = Maps.newHashMap();
        filterInitParam.put("casServerUrlPrefix", config.get(KEY_CAS_SERVER_URL_PREFIX));
        filterInitParam.put("serverName", config.get(KEY_CAS_REGISTER_SERVER_NAME));
        filterInitParam.put("encoding", "UTF-8");
        //we don't want to use session to store login information, since we will be deployed to a cluster, not a single instance
        filterInitParam.put("useSession", "false");
        filterInitParam.put("useRedis", "true");
        filterInitParam.put("redisClusterName", "casClientPrincipal");

        casValidationFilter
            .setFilter(filter("org.jasig.cas.client.validation.Cas20ProxyReceivingTicketValidationFilter"));
        casValidationFilter.setInitParameters(filterInitParam);
        casValidationFilter.addUrlPatterns("/*");

        return casValidationFilter;

    }


    @Bean
    public FilterRegistrationBean assertionHolder() {

        FilterRegistrationBean assertionHolderFilter = new FilterRegistrationBean();

        Map<String, String> filterInitParam = Maps.newHashMap();

        assertionHolderFilter.setInitParameters(filterInitParam);

        assertionHolderFilter
            .setFilter(new XPipeAssertionThreadLocalFilter());
        assertionHolderFilter.addUrlPatterns("/*");

        return assertionHolderFilter;
    }


    private Filter filter(String className) {
        Class clazz = null;
        try {
            clazz = Class.forName(className);
            Object obj = clazz.newInstance();
            return (Filter) obj;
        } catch (Exception e) {
            throw new RuntimeException("instance filter fail", e);
        }

    }

    private EventListener listener(String className) {
        Class clazz = null;
        try {
            clazz = Class.forName(className);
            Object obj = clazz.newInstance();
            return (EventListener) obj;
        } catch (Exception e) {
            throw new RuntimeException("instance listener failed", e);
        }
    }


    @Bean
    public ServletContextInitializer servletContextInitializer() {

        return new ServletContextInitializer() {

            @Override
            public void onStartup(ServletContext servletContext) throws ServletException {
                String loggingServerIP = config.get(KEY_CLOGGING_SERVER_URL);
                String loggingServerPort = config.get(KEY_CLOGGING_SERVER_PORT);
                String credisServiceUrl = config.get(KEY_CREDIS_SERVER_URL);
                servletContext.setInitParameter("loggingServerIP",
                                                Strings.isNullOrEmpty(loggingServerIP) ? "" : loggingServerIP);
                servletContext.setInitParameter("loggingServerPort",
                                                Strings.isNullOrEmpty(loggingServerPort) ? "" : loggingServerPort);
                servletContext.setInitParameter("credisServiceUrl",
                                                Strings.isNullOrEmpty(credisServiceUrl) ? "" : credisServiceUrl);
            }
        };
    }

}
