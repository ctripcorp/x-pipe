package com.ctrip.xpipe.retry;

import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.api.retry.RetryPolicy;

/**
 * @author shyin
 *         <p>
 *         Sep 20, 2016
 */
public class RestOperationsRetryPolicy extends AbstractRetryPolicy implements RetryPolicy {

    private int retryInterval;

    public RestOperationsRetryPolicy() {
        this(5);
    }

    public RestOperationsRetryPolicy(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    @Override
    public boolean retry(Throwable e) {
        if (e instanceof ResourceAccessException) {
            return true;
        }
        if (e instanceof HttpServerErrorException) {
            HttpStatus statusCode = ((HttpServerErrorException) e).getStatusCode();
            if (statusCode == HttpStatus.BAD_GATEWAY) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected int getSleepTime(int currentRetryTime) {
        return (retryInterval <= 0) ? 0 : retryInterval;
    }

}
