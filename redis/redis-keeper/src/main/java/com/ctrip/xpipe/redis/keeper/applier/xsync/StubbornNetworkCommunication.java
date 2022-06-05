package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 14:44
 */
public interface StubbornNetworkCommunication extends NetworkCommunication {

    Logger logger = LoggerFactory.getLogger(StubbornNetworkCommunication.class);

    ScheduledExecutorService scheduled();

    long reconnectDelayMillis();

    boolean closed();

    /* API */

    @Override
    default void connect(Endpoint endpoint) {

        changeTarget(endpoint);

        try {
            Command<Object> command = connectCommand();
            command.future().addListener((f)->{
                scheduleReconnect();
            });
            command.execute();
        } catch (Throwable t) {
            logger.error("[doConnect() fail] " + endpoint());
            scheduleReconnect();
        }
    }

    default void scheduleReconnect() {

        scheduled().schedule(() -> {
            if (!closed()) {
                connect(endpoint());
            }
        }, reconnectDelayMillis(), TimeUnit.MILLISECONDS);
    }
}
