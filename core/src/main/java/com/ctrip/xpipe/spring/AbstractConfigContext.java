package com.ctrip.xpipe.spring;

import org.springframework.context.annotation.Bean;

import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;

/**
 * @author wenchao.meng
 *
 * Jun 25, 2016
 */
public abstract class AbstractConfigContext {

	@Bean
	public SpringComponentLifecycleManager getSpringComponentLifecycleManager(){
		return new SpringComponentLifecycleManager();
	}

}
