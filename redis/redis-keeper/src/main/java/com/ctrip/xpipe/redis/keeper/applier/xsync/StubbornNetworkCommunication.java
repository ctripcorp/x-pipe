package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 14:44
 */
public abstract class StubbornNetworkCommunication extends AbstractInstanceComponent implements NetworkCommunication {

    private Logger logger = LoggerFactory.getLogger(StubbornNetworkCommunication.class);

    /* keep scheduler and user-call safe */
    private Object lock = new Object();
    private AtomicBoolean firstCall = new AtomicBoolean(true);

    /* customized actions & resource */
    protected abstract ScheduledExecutorService scheduled();
    protected abstract void initState(Endpoint endpoint, Object... states);
    protected abstract void refreshStateWhenReconnect();
    protected abstract Command<Object> connectCommand() throws Exception;
    protected abstract void doDisconnect() throws Exception;
    protected abstract boolean closed();

    private boolean changeTarget(Endpoint endpoint, Object... states) {
        if (Objects.equals(endpoint(), endpoint)) {
            return false;
        }

        initState(endpoint, states);
        return true;
    }

    @Override
    public void connect(Endpoint endpoint, Object... states) {

        synchronized (lock) {

            if (!changeTarget(endpoint, states)) return;
            disconnect(); // close and reconnect later by scheduleReconnect()

            if (firstCall.compareAndSet(true, false)) {
                doConnect();
            }
        }
    }

    @Override
    public void disconnect() {
        try {
            doDisconnect();
        } catch (Throwable t) {
            logger.error("[doDisconnect() fail]  {}", endpoint(), t);
        }
    }

    private void doConnect() {
        if (endpoint() == null) {
            scheduleReconnect();
            return;
        }
        try {
            Command<Object> command = connectCommand();
            command.future().addListener((f) -> {

                logger.info("[future.done()] isSuccess: {}", f.isSuccess());

                if (!f.isSuccess()) {
                    logger.error("[future.done()] fail", f.cause());
                }

                scheduleReconnect();
            });

            logger.info("[doConnect() try execute] {}", endpoint());

            command.execute();
        } catch (Throwable t) {

            logger.error("[doConnect() fail] {}", endpoint());
            logger.error("[doConnect() fail]", t);

            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {

        scheduled().schedule(() -> {

            synchronized (lock) {

                if (!closed()) {
                    refreshStateWhenReconnect();
                    doConnect();
                }
            }
        }, 2000, TimeUnit.MILLISECONDS);
    }
}
