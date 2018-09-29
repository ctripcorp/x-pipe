package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.retry.RetryDelay;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 16, 2018
 */
public class DefaultRetryCommandFactory<V> implements RetryCommandFactory<V> {

    private int retryTimes = -1;
    private int retryTimeoutMilli = -1;
    private RetryPolicy retryPolicy;
    private ScheduledExecutorService scheduled;
    private AtomicBoolean destroy = new AtomicBoolean(false);

    public DefaultRetryCommandFactory() {
    }

    public DefaultRetryCommandFactory(int retryTimes, RetryPolicy retryPolicy, ScheduledExecutorService scheduled) {
        this.retryPolicy = retryPolicy;
        this.retryTimes = retryTimes;
        this.scheduled = scheduled;
    }

    public DefaultRetryCommandFactory(RetryPolicy retryPolicy, int retryTimeoutMilli, ScheduledExecutorService scheduled) {
        this.retryPolicy = retryPolicy;
        this.retryTimeoutMilli = retryTimeoutMilli;
        this.scheduled = scheduled;
    }

    public static <V> RetryCommandFactory noRetryFactory() {
        return new DefaultRetryCommandFactory<V>();
    }

    public static <V> RetryCommandFactory retryForever(ScheduledExecutorService scheduled, int retryDelayMilli) {

        return new DefaultRetryCommandFactory<V>(Integer.MAX_VALUE, new RetryDelay(retryDelayMilli), scheduled);
    }

    public static <V> RetryCommandFactory retryNTimes(ScheduledExecutorService scheduled, int retryTimes, int retryDelayMilli) {
        return new DefaultRetryCommandFactory<V>(retryTimes, new RetryDelay(retryDelayMilli), scheduled);
    }

    @Override
    public Command<V> createRetryCommand(Command<V> command) {
        return new CommandRetryWrapper(scheduled, retryTimes, retryTimeoutMilli, createRetryPolicy(), command);
    }

    private RetryPolicy createRetryPolicy() {
        return (RetryPolicy) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[]{RetryPolicy.class},
                new RetryHandler());
    }

    @Override
    public void destroy() throws Exception {
        destroy.set(true);
    }


    public class RetryHandler implements InvocationHandler {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

            if (method.getName().equals("retry")) {
                if(retryPolicy == null){
                    return false;
                }
                if (destroy.get()) {
                    return false;
                }
            }
            return method.invoke(retryPolicy, args);
        }
    }


}
