package com.ctrip.xpipe.monitor;

import com.ctrip.xpipe.spring.AbstractProfile;
import com.dianping.cat.servlet.CatFilter;
import com.dianping.cat.servlet.CatListener;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.servlet.DispatcherType;

/**
 * @author wenchao.meng
 *
 *         Aug 11, 2016
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class CatConfig {

	public static final String CAT_ENABLED_KEY = "cat.client.enabled";
	
	private static final boolean catEnabled = Boolean.parseBoolean(System.getProperty(CAT_ENABLED_KEY, "true"));
	
	public static boolean isCatenabled() {
		return catEnabled;
	}

	@Bean
	public FilterRegistrationBean catFilter() {
		
		FilterRegistrationBean bean = new FilterRegistrationBean();
		bean.setFilter(new CatFilter());
		bean.setName("cat-filter");
		bean.addUrlPatterns("/*");
		bean.setDispatcherTypes(DispatcherType.REQUEST, DispatcherType.FORWARD);
		return bean;
	}

	@Bean(name="cat-listener")
	public ServletListenerRegistrationBean<CatListener> catListener() {
		return new ServletListenerRegistrationBean<>(new CatListener());
	}
}
