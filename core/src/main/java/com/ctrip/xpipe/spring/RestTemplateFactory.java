package com.ctrip.xpipe.spring;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.retry.RetryNTimes;
import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.retry.RetryPolicyFactory;

/**
 * @author wenchao.meng
 *
 *         Aug 5, 2016
 */
public class RestTemplateFactory {

	public static RestTemplate createRestTemplate() {

		return new RestTemplate();
	}

	public static RestOperations createCommonsHttpRestTemplate() {
		return createCommonsHttpRestTemplate(10, 100, 5000, 5000, 2,
				RetryPolicyFactories.newRestOperationsRetryPolicyFactory(10));
	}

	public static RestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
			int connectTimeout, int soTimeout) {
		return createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, connectTimeout, soTimeout, 2,
				RetryPolicyFactories.newRestOperationsRetryPolicyFactory(10));
	}

	public static RestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
			int connectTimeout, int soTimeout, int retryTimes, RetryPolicyFactory retryPolicyFactory) {

		HttpClient httpClient = HttpClientBuilder.create().setMaxConnPerRoute(maxConnPerRoute)
				.setMaxConnTotal(maxConnTotal)
				.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(soTimeout).build())
				.setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectTimeout).build()).build();
		ClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient);
		RestTemplate restTemplate = new RestTemplate(factory);

		return (RestOperations) Proxy.newProxyInstance(RestOperations.class.getClassLoader(),
				new Class[] { RestOperations.class },
				new RetryableRestOperationsHandler(restTemplate, retryTimes, retryPolicyFactory));
	}

	private static class RetryableRestOperationsHandler implements InvocationHandler {
		private RestTemplate restTemplate;
		private int retryTimes;
		private RetryPolicyFactory retryPolicyFacotry;

		public RetryableRestOperationsHandler(RestTemplate restTemplate, int retryTimes,
				RetryPolicyFactory retryPolicyFactory) {
			this.restTemplate = restTemplate;
			this.retryTimes = retryTimes;
			this.retryPolicyFacotry = retryPolicyFactory;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			return retryableInvoke(restTemplate, method, args);
		}

		private Object retryableInvoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
			final RetryPolicy retryPolicy = retryPolicyFacotry.create();

			// for (int i = 0; retryTimes == -1 || i != retryTimes; ++i) {
			// if (i >= 1) {
			// retryPolicy.retryWaitMilli(true);
			// }
			//
			// try {
			// return method.invoke(proxy, args);
			// } catch (Exception e) {
			// if (!retryPolicy.retry(e.getCause()) || i == retryTimes - 1) {
			// throw e.getCause();
			// }
			// }
			// }
			// return null;
			return new RetryNTimes<Object>(retryTimes, retryPolicy) {
				@Override
				public Object execute(Command<Object> command) throws Exception {

					for (int i = 0; retryTimes == -1 || i <= retryTimes; i++) {

						if (i >= 1) {
							logger.info("[execute][retry]{}, {}", i, command);
							retryPolicy.retryWaitMilli(true);
						}

						try {
							return command.execute().get(retryPolicy.waitTimeoutMilli(), TimeUnit.MILLISECONDS);
						} catch (Exception e) {
							ExceptionUtils.logException(logger, e,
									String.format("cmd:%s, message:%s", command, e.getMessage()));
							if (!retryPolicy.retry(e.getCause()) || i == retryTimes) {
								logger.info("[execute][no retry]", e);
								// break;
								throw (Exception) e.getCause();
							}
						}
						command.reset();
					}
					return null;
				}

			}.execute(new AbstractCommand<Object>() {
				@Override
				public String getName() {
					return "RetryableRestInvoke";
				}

				@Override
				protected void doExecute() throws Exception {
					try {
						future().setSuccess(method.invoke(proxy, args));
					} catch (Exception e) {
						throw (Exception) e.getCause();
					}
				}

				@Override
				protected void doReset() {

				}
			});
		}
	}

}
