package com.ctrip.xpipe.spring;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public class RestTemplateFactory {
	
	public static RestTemplate createRestTemplate(){
		
		return new RestTemplate();
	}

	public static RestTemplate createCommonsHttpRestTemplate(){
		return createCommonsHttpRestTemplate(10, 100, 5000, 5000);
	}

	public static RestTemplate createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal, int connectTimeout, int soTimeout){
	
		HttpClient httpClient = HttpClientBuilder.create()
				.setMaxConnPerRoute(maxConnPerRoute)
				.setMaxConnTotal(maxConnTotal)
				.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(soTimeout).build())
				.setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectTimeout).build())
				.build();
		ClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient); 
		return new RestTemplate(factory);
	}

}
