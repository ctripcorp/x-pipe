package com.ctrip.xpipe.redis.core.metaserver.impl;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.google.common.base.Function;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public abstract class AbstractMetaService implements MetaServerService{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected RestTemplate restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();


	protected <T> T pollMetaServer(Function<String, T> fun) {
		
		List<String> metaServerList = getMetaServerList();

		for (String url : metaServerList) {
			T result = fun.apply(url);
			if (result != null) {
				return result;
			} else {
				continue;
			}
		}
		return null;
	}

	protected abstract List<String> getMetaServerList();
}
