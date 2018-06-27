package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class DefaultEndpointHealthChecker implements EndpointHealthChecker {

    private static final int TIMEOUT_MILLI = 1000 * 5;

    private static final Logger logger = LoggerFactory.getLogger(DefaultEndpointHealthChecker.class);

    @Override
    public boolean checkConnectivity(Endpoint endpoint) {
        CommandFuture<Boolean> future = new TcpPortCheckCommand(endpoint.getHost(), endpoint.getPort()).execute();

        try {
            return future.get(TIMEOUT_MILLI, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("[checkConnectivity] ", e);
        }
        return false;
    }
}
