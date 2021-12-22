package com.ctrip.xpipe.service.sso;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.Filter;
import javax.servlet.Servlet;


/**
 * @author lepdou 2016-11-08
 */
@Configuration
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SSOConfigurations {

    @Bean
    public FilterRegistrationBean ctripSSOFilter() {
        FilterRegistrationBean ctripSSOFilter = new FilterRegistrationBean();
        ctripSSOFilter.setFilter(new CtripSSOFilter());
        ctripSSOFilter.addUrlPatterns("/*");
        return ctripSSOFilter;
    }

    @Bean
    public FilterRegistrationBean assertionHolderFilter() {
        FilterRegistrationBean assertionHolderFilter = new FilterRegistrationBean();
        assertionHolderFilter.setFilter(filter("com.ctrip.infosec.sso.client.util.AssertionThreadLocalFilter"));
        assertionHolderFilter.addUrlPatterns("/*");
        return assertionHolderFilter;
    }

    @Bean
    public ServletRegistrationBean logoutServlet() {
        ServletRegistrationBean logoutServlet = new ServletRegistrationBean();
        logoutServlet.setServlet(servlet("com.ctrip.infosec.sso.client.logout.Logout"));
        logoutServlet.addUrlMappings("/logout");
        return logoutServlet;
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

    private Servlet servlet(String className) {
        Class clazz = null;
        try {
            clazz = Class.forName(className);
            Object obj = clazz.newInstance();
            return (Servlet) obj;
        } catch (Exception e) {
            throw new RuntimeException("instance servlet fail", e);
        }

    }

}
