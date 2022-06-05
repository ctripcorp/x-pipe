package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 15:15
 */
public interface NetworkCommunication extends NetworkCommunicationState {

    Logger logger = LoggerFactory.getLogger(NetworkCommunication.class);

    Command<Object> connectCommand() throws Exception;

    void doDisconnect() throws Exception;

    default void changeTarget(Endpoint endpoint) {
        if (isConnected()) {
            if (endpoint().equals(endpoint)) {
                return;
            } else {
                disconnect();
            }
        }

        setHostPort(endpoint());
    }

    /* API */

    default void connect(Endpoint endpoint) {

        changeTarget(endpoint);

        try {
            connectCommand().execute();
        } catch (Throwable t) {
            logger.error("[doConnect() fail] " + endpoint());
        }
    }

    default void disconnect() {
        try {
            doDisconnect();
        } catch (Throwable t) {
            logger.error("[doDisconnect() fail] " + endpoint());
        }
    }
}
