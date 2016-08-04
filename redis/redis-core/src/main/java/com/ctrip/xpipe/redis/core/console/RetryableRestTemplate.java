package com.ctrip.xpipe.redis.core.console;


import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.SocketTimeoutException;

import javax.annotation.PostConstruct;

@Component
public class RetryableRestTemplate {

	private static final int RETRY_TIMES = 3;

	private RestTemplate restTemplate;

	@Autowired
	private RestTemplateFactory restTemplateFactory;


	@PostConstruct
	private void postConstruct() {
		restTemplate = restTemplateFactory.getObject();
	}

	public <T> T get(String path, Class<T> responseType, Object... urlVariables)
		throws RestClientException {
		return execute(HttpMethod.GET, path, null, responseType, urlVariables);
	}

	public <T> T post(String path, Object request, Class<T> responseType, Object... uriVariables)
		throws RestClientException {
		return execute(HttpMethod.POST, path, request, responseType, uriVariables);
	}

	public void put(String path, Object request, Object... urlVariables) throws RestClientException {
		execute(HttpMethod.PUT, path, request, null, urlVariables);
	}

	public void delete(String path, Object... urlVariables) throws RestClientException {
		execute(HttpMethod.DELETE, path, null, null, urlVariables);
	}

	private <T> T execute(HttpMethod method, String path, Object request, Class<T> responseType,
						  Object... uriVariables) {

		if (path.startsWith("/")) {
			path = path.substring(1, path.length());
		}

		Throwable throwable = null;
		for (int i = 0; i < RETRY_TIMES; i++) {
			try {

				T result = doExecute(method, path, request, responseType, uriVariables);
				return result;
			} catch (Throwable t) {
				throwable = t;
				if (canRetry(t, method)) {
					continue;
				} else {//biz exception rethrow
					throw t;
				}
			}
		}

		throw new RedisRuntimeException("console server error", throwable);
	}

	private <T> T doExecute(HttpMethod method, String path, Object request,
							Class<T> responseType,
							Object... uriVariables) {
		T result = null;
		switch (method) {
			case GET:
				result = restTemplate.getForObject(host() + path, responseType, uriVariables);
				break;
			case POST:
				result =
					restTemplate.postForEntity(host() + path, request, responseType, uriVariables).getBody();
				break;
			case PUT:
				restTemplate.put(host() + path, request, uriVariables);
				break;
			case DELETE:
				restTemplate.delete(host() + path, uriVariables);
				break;
			default:
				throw new UnsupportedOperationException(String.format("not supported http method(method=%s)", method));
		}
		return result;
	}

	private String host() {
		return "localhost:8080";
	}

	//post,delete,put处理超时情况下不重试
	private boolean canRetry(Throwable e, HttpMethod method) {
		Throwable nestedException = e.getCause();
		if (method == HttpMethod.GET) {
			return nestedException instanceof SocketTimeoutException
				   || nestedException instanceof HttpHostConnectException
				   || nestedException instanceof ConnectTimeoutException;
		} else {
			return nestedException instanceof HttpHostConnectException
				   || nestedException instanceof ConnectTimeoutException;
		}
	}

}
