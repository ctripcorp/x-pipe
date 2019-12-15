package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.retry.RetryNTimes;
import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.retry.RetryPolicyFactory;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.springframework.http.client.AsyncClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsAsyncClientHttpRequestFactory;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;
import org.springframework.web.client.AsyncRestOperations;
import org.springframework.web.client.AsyncRestTemplate;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class AsyncRestTemplateFactory extends AbstractRestTemplateFactory {

    public static AsyncRestOperations createCommonsHttpRestTemplateWithRetry(int retryTimes, int retryIntervalMilli, int connectionTimeout, int soTimeout) {
        return createCommonsHttpRestTemplate(100, 1000, OsUtils.getCpuCount(), connectionTimeout, soTimeout, retryTimes,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(retryIntervalMilli));
    }


    public static AsyncRestOperations createCommonsHttpRestTemplateWithRetry(int retryTimes, int retryIntervalMilli) {
        return createCommonsHttpRestTemplate(100, 1000, OsUtils.getCpuCount(), 5000, 5000, retryTimes,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(retryIntervalMilli));
    }

    public static AsyncRestOperations createCommonsHttpRestTemplate() {

        return createCommonsHttpRestTemplateWithRetry(0, 10);
    }

    public static AsyncRestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
                                                               int connectTimeout, int soTimeout) {
        return createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, OsUtils.getCpuCount(), connectTimeout, soTimeout, 0,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(10));
    }

    public static AsyncRestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal,
                                                               int connectTimeout, int soTimeout, int retryTimes) {
        return createCommonsHttpRestTemplate(maxConnPerRoute, maxConnTotal, OsUtils.getCpuCount(), connectTimeout, soTimeout, retryTimes,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(10));
    }

    public static AsyncRestOperations createCommonsHttpRestTemplate(int maxConnPerRoute, int maxConnTotal, int ioThreads,
                                                                    int connectTimeout, int soTimeout, int retryTimes, RetryPolicyFactory retryPolicyFactory) {

        IOReactorConfig config = IOReactorConfig.custom()
                .setTcpNoDelay(true)
                .setSoTimeout(soTimeout)
                .setConnectTimeout(connectTimeout)
                .setIoThreadCount(ioThreads)
                .setSoKeepAlive(true)
                .build();

        CloseableHttpAsyncClient httpAsyncClient = HttpAsyncClients.custom()
                .setMaxConnPerRoute(maxConnPerRoute)
                .setMaxConnTotal(maxConnTotal)
                .setDefaultIOReactorConfig(config)
                .setThreadFactory(XpipeThreadFactory.create("XPIPE-ASYNC-REST-CLIENT"))
                .build();

        httpAsyncClient.start();

        AsyncClientHttpRequestFactory factory = new HttpComponentsAsyncClientHttpRequestFactory(httpAsyncClient);

        AsyncRestTemplate asyncRestTemplate = new AsyncRestTemplate(factory);
        setXPipeSafeJacksonMapper(asyncRestTemplate.getMessageConverters());


        return (AsyncRestOperations) Proxy.newProxyInstance(AsyncRestOperations.class.getClassLoader(),
                new Class[]{AsyncRestOperations.class},
                new RetryableRestOperationsHandler(asyncRestTemplate, retryTimes, retryPolicyFactory));
    }

    private static class RetryableRestOperationsHandler implements InvocationHandler {

        private AsyncRestTemplate asyncRestTemplate;

        private int retryTimes;

        private RetryPolicyFactory retryPolicyFactory;

        public RetryableRestOperationsHandler(AsyncRestTemplate asyncRestTemplate, int retryTimes,
                                              RetryPolicyFactory retryPolicyFactory) {
            this.asyncRestTemplate = asyncRestTemplate;
            this.retryTimes = retryTimes;
            this.retryPolicyFactory = retryPolicyFactory;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
            return retryableInvoke(asyncRestTemplate, method, args);
        }

        public Object retryableInvoke(final AsyncRestTemplate proxy, final Method method, final Object[] args) throws Exception {
            final RetryPolicy retryPolicy = retryPolicyFactory.create();

            return new RetryNTimes<Object>(retryTimes, retryPolicy).execute(new AbstractCommand<Object>() {

                @Override
                public String getName() {
                    return String.format("[retryable-invoke]%s(%s)", method.getName(), (args.length >= 1 ? args[0] : ""));
                }

                @Override
                protected void doExecute() throws Exception {
                    Object result = method.invoke(proxy, args);
                    if (result instanceof ListenableFuture) {
                        ListenableFuture listenableFuture = (ListenableFuture) result;
                        listenableFuture.addCallback(new SuccessCallback() {
                            @Override
                            public void onSuccess(Object o) {
                                future().setSuccess(o);
                            }
                        }, new FailureCallback() {
                            @Override
                            public void onFailure(Throwable throwable) {
                                future().setFailure(throwable);
                            }
                        });
                    } else {
                        future().setSuccess(method.invoke(proxy, args));
                    }
                }

                @Override
                protected void doReset() {

                }
            });
        }

    }
}
