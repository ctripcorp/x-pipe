package com.ctrip.xpipe.redis.core.console;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class RestTemplateFactory implements FactoryBean<RestTemplate>, InitializingBean {

	private RestTemplate restTemplate;

	@Override
	public RestTemplate getObject(){
		return restTemplate;
	}

	@Override
	public Class<?> getObjectType() {
		return RestTemplate.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet(){
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		restTemplate = new RestTemplate();
		HttpComponentsClientHttpRequestFactory requestFactory =
			new HttpComponentsClientHttpRequestFactory(httpClient);
		requestFactory.setConnectTimeout(3000);
		requestFactory.setReadTimeout(10000);

		restTemplate.setRequestFactory(requestFactory);
	}
}
