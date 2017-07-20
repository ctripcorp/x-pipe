package com.ctrip.xpipe.spring;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.retry.RetryNTimes;
import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.retry.RetryPolicyFactory;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 5, 2016
 */
public class RestTemplateFactory {

    public static RestTemplate createRestTemplate() {

        return new RestTemplate();
    }

    public static RestOperations createCommonsHttpRestTemplate(int retryTimes, int retryIntervalMilli) {
        return createCommonsHttpRestTemplate(100, 1000, 5000, 5000, retryTimes,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(retryIntervalMilli));
    }


    public static RestOperations createCommonsHttpRestTemplate() {

        return createCommonsHttpRestTemplate(0, 10);
    }

    public static RestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
                                                               int connectTimeout, int soTimeout) {
        return createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, connectTimeout, soTimeout, 0,
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
        ClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient);
        RestTemplate restTemplate = new RestTemplate(factory);
        for (HttpMessageConverter<?> hmc : restTemplate.getMessageConverters()) {
            if (hmc instanceof MappingJackson2HttpMessageConverter) {
                MappingJackson2HttpMessageConverter mj2hmc = (MappingJackson2HttpMessageConverter) hmc;
                mj2hmc.setObjectMapper((new ObjectMapper()).configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true));
            }
        }

        return (RestOperations) Proxy.newProxyInstance(RestOperations.class.getClassLoader(),
                new Class[]{RestOperations.class},
                new RetryableRestOperationsHandler(restTemplate, retryTimes, retryPolicyFactory));
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

            return new RetryNTimes<Object>(retryTimes, retryPolicy).execute(new AbstractCommand<Object>() {

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
