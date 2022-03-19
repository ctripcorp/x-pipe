package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.retry.RetryNTimes;
import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.retry.RetryPolicyFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 5, 2016
 */
public class RestTemplateFactory {

    private static final Logger logger = LoggerFactory.getLogger(RestTemplateFactory.class);

    public static RestTemplate createRestTemplate() {

        return new RestTemplate();
    }

    public static RestOperations createCommonsHttpRestTemplateWithRetry(int retryTimes, int retryIntervalMilli, int connectionTimeout, int soTimeout) {
        return createCommonsHttpRestTemplate(100, 1000, connectionTimeout, soTimeout, retryTimes,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(retryIntervalMilli));
    }


    public static RestOperations createCommonsHttpRestTemplateWithRetry(int retryTimes, int retryIntervalMilli) {
        return createCommonsHttpRestTemplate(100, 1000, 5000, 5000, retryTimes,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(retryIntervalMilli));
    }


    public static RestOperations createCommonsHttpRestTemplate() {

        return createCommonsHttpRestTemplateWithRetry(0, 10);
    }

    public static RestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
                                                               int connectTimeout, int soTimeout) {
        return createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, connectTimeout, soTimeout, 0,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(10));
    }

    public static RestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
                                                               int connectTimeout, int soTimeout, int retryTimes) {
        return createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, connectTimeout, soTimeout, retryTimes,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(10));
    }

    public static RestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
                                                               int connectTimeout, int soTimeout, int retryTimes, RetryPolicyFactory retryPolicyFactory) {
        HttpClient httpClient = HttpClientBuilder.create()
                .setMaxConnPerRoute(maxConnPerRoute)
                .setMaxConnTotal(maxConnTotal)
                .setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(soTimeout).build())
                .setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectTimeout).build())
                .build();
        ClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient) {
            @Override
            public ClientHttpRequest createRequest(URI uri, HttpMethod httpMethod) throws IOException {
                logger.debug("[rest][{}] {}", httpMethod, uri);
                return super.createRequest(uri, httpMethod);
            }
        };
        RestTemplate restTemplate = new RestTemplate(factory);

        List<HttpMessageConverter<?>> converters = new ArrayList<>();
        //set jackson mapper
        for (HttpMessageConverter<?> hmc : restTemplate.getMessageConverters()) {
            if (hmc instanceof MappingJackson2HttpMessageConverter) {
                ObjectMapper objectMapper = createObjectMapper();
                MappingJackson2HttpMessageConverter mj2hmc = (MappingJackson2HttpMessageConverter) hmc;
                mj2hmc.setObjectMapper(objectMapper);
            }
            if (!(hmc instanceof MappingJackson2XmlHttpMessageConverter)) {
                converters.add(hmc);
            }
        }

        restTemplate.setMessageConverters(converters);
        return (RestOperations) Proxy.newProxyInstance(RestOperations.class.getClassLoader(),
                new Class[]{RestOperations.class},
                new RetryableRestOperationsHandler(restTemplate, retryTimes, retryPolicyFactory));
    }

    private static ObjectMapper createObjectMapper() {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        return objectMapper;

    }

    private static class RetryableRestOperationsHandler implements InvocationHandler {

        private RestTemplate restTemplate;

        private int retryTimes;

        private RetryPolicyFactory retryPolicyFactory;

        public RetryableRestOperationsHandler(RestTemplate restTemplate, int retryTimes,
                                              RetryPolicyFactory retryPolicyFactory) {
            this.restTemplate = restTemplate;
            this.retryTimes = retryTimes;
            this.retryPolicyFactory = retryPolicyFactory;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
            return retryableInvoke(restTemplate, method, args);
        }

        public Object retryableInvoke(final Object proxy, final Method method, final Object[] args) throws Exception {
            final RetryPolicy retryPolicy = retryPolicyFactory.create();

            return new RetryNTimes<Object>(retryTimes, retryPolicy, false).execute(new AbstractCommand<Object>() {

                @Override
                public String getName() {
                    return String.format("[retryable-invoke]%s(%s)", method.getName(), (args.length >= 1 ? args[0] : ""));
                }

                @Override
                protected void doExecute() throws Exception {
                    future().setSuccess(method.invoke(proxy, args));
                }

                @Override
                protected void doReset() {

                }

            });
        }

    }

}
