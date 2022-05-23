package com.ctrip.xpipe.service.sso;

import com.ctrip.infosec.sso.client.logout.Logout;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Collection;


/**
 * @author lepdou 2016-11-08
 */
@Configuration
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SSOConfigurations {

    @Bean
    public FilterRegistrationBean ctripSSOFilter() {
        FilterRegistrationBean ctripSSOFilter = new FilterRegistrationBean();
        ctripSSOFilter.setFilter(new XPipeSSOFilter());
        ctripSSOFilter.addUrlPatterns("/*");
        return ctripSSOFilter;
    }

    @Bean
    public ServletRegistrationBean logoutServletRegistration() {

        ServletRegistrationBean registration = new ServletRegistrationBean();
        registration.setServlet(new Logout());
        Collection<String> urlMappings = new ArrayList<>();
        urlMappings.add("/logout");
        registration.setUrlMappings(urlMappings);
        registration.setLoadOnStartup(2);
        return registration;
    }

}
