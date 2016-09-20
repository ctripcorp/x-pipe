package com.ctrip.xpipe.spring;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.retry.AbstractRetryPolicyFactory;
import com.ctrip.xpipe.retry.RetryPolicyFactoryProducer;
import com.ctrip.xpipe.retry.RetryPolicyFactoryType;

/**
 * @author wenchao.meng
 *
 *         Aug 5, 2016
 */
public class RestTemplateFactory {
	private static Logger logger = LoggerFactory.getLogger(RestTemplateFactory.class);

	public static RestTemplate createRestTemplate() {

		return new RestTemplate();
	}

	public static RestOperations createCommonsHttpRestTemplate() {
		return createCommonsHttpRestTemplate(10, 100, 5000, 5000, RetryPolicyFactoryProducer
				.getRetryPolicyFactory(RetryPolicyFactoryType.REST_OPERATIONS_RETRY_POLICY, "10", "300"));
	}

	public static RestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
			int connectTimeout, int soTimeout) {
		return createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, connectTimeout, soTimeout,
				RetryPolicyFactoryProducer.getRetryPolicyFactory(RetryPolicyFactoryType.REST_OPERATIONS_RETRY_POLICY,
						"10", "300"));
	}

	public static RestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
			int connectTimeout, int soTimeout, AbstractRetryPolicyFactory retryPolicyFactory) {

		HttpClient httpClient = HttpClientBuilder.create().setMaxConnPerRoute(maxConnPerRoute)
				.setMaxConnTotal(maxConnTotal)
				.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(soTimeout).build())
				.setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectTimeout).build()).build();
		ClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient);
		RestTemplate restTemplate = new RestTemplate(factory);

		return (RestOperations) Proxy.newProxyInstance(RestOperations.class.getClassLoader(),
				new Class[] { RestOperations.class },
				new RetryableRestOperationsHandler(restTemplate, retryPolicyFactory));
	}

	private static class RetryableRestOperationsHandler implements InvocationHandler {

		private RestTemplate restTemplate;

		private AbstractRetryPolicyFactory retryPolicyFactory;

		public RetryableRestOperationsHandler(RestTemplate restTemplate,
				AbstractRetryPolicyFactory retryPolicyFactory) {
			this.restTemplate = restTemplate;
			this.retryPolicyFactory = retryPolicyFactory;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			return retryableInvoke(restTemplate, method, args);
		}

		public Object retryableInvoke(Object proxy, Method method, Object[] args) throws Throwable {
			RetryPolicy retryPolicy = retryPolicyFactory.createRetryPolicy();
			while (true) {
				try {
					logger.info("[retryableInvoke][retryTimes]{}", retryPolicy.getRetryTimes());
					return method.invoke(proxy, args);
				} catch (Exception e) {
					if (retryPolicy.retry(e.getCause())) {
						retryPolicy.retryWaitMilli(true);
						continue;
					}
					throw e.getCause();
				}
			}
		}

	}

}
