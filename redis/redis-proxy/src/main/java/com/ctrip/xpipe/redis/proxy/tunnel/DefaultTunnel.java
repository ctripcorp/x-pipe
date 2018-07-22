package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.handler.TunnelTrafficReporter;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.session.*;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.tunnel.state.*;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultTunnel extends AbstractLifecycleObservable implements Tunnel {

    private static final String STATE_CHANGE = "Tunnel.State.Change";

    private static final Logger logger = LoggerFactory.getLogger(DefaultTunnel.class);

    private final String identity;

    private Channel frontendChannel;

    private volatile FrontendSession frontend;

    private volatile BackendSession backend;

    private ProxyProtocol protocol;

    private AtomicReference<TunnelState> tunnelState = new AtomicReference<>(new TunnelHalfEstablished(this));

    private ProxyConfig config;

    private ProxyResourceManager proxyResourceManager;

    public DefaultTunnel(Channel frontendChannel, ProxyProtocol protocol, ProxyConfig config,
                         ProxyResourceManager proxyResourceManager) {

        this.config = config;
        this.protocol = protocol;
        this.frontendChannel = frontendChannel;
        this.proxyResourceManager = proxyResourceManager;
        this.identity = protocol.getFinalStation();
    }

    @Override
    public String identity() {
        return this.identity;
    }

    @Override
    public void forwardToBackend(ByteBuf message) {
        backend().tryWrite(message);
    }

    @Override
    public void forwardToFrontend(ByteBuf message) {
        frontend().tryWrite(message);
    }

    @Override
    public FrontendSession frontend() {
        return frontend;
    }

    @Override
    public BackendSession backend() {
        return backend;
    }

    @Override
    public TunnelMeta getTunnelMeta() {
        return new TunnelMeta(this, protocol);
    }

    @Override
    public TunnelState getState() {
        return this.tunnelState.get();
    }

    @VisibleForTesting
    protected void setState(TunnelState newState) {
        if(!tunnelState.get().isValidNext(newState)) {
            logger.debug("[setState] Set state failed, state relationship not match, old: {}, new: {}",
                    tunnelState.get().name(), newState.name());
            return;
        }
        TunnelState oldState = this.tunnelState.getAndSet(newState);
        if (!oldState.equals(newState)) {
            logger.info("[setState] tunnel state change: {} -> {} ({})", oldState, newState, getTunnelMeta());
            EventMonitor.DEFAULT.logEvent(STATE_CHANGE, String.format("Tunnel state change: %s -> %s", oldState, newState));
            notifyObservers(new TunnelStateChangeEvent(oldState, newState));
        } else {
            logger.debug("[setState] already the same state: {}", oldState);
        }

    }

    @Override
    public ProxyProtocol getProxyProtocol() {
        return protocol;
    }

    /**
     * Observe for session change, see @DefaultSessionStore frontend() & backend()
     * receives SessionStateChangeEvent | TunnelStateChangeEvent*/
    @Override
    public void update(Object args, Observable observable) {
        if(!(observable instanceof Session)) {
            logger.error("[update] Tunnel should only observe session, not {}", observable.getClass().getName());
            return;
        }
        Session session = (Session) observable;
        SessionStateChangeEvent event = (SessionStateChangeEvent) args;
        if(event.getCurrent() instanceof SessionClosed) {
            onSessionClosed(session);
        } else if(event.getCurrent() instanceof SessionEstablished) {
            onSessionEstablished(session);
        } else {
            logger.info("[update] un-recognised event: {}", event);
        }
    }

    private void onSessionClosed(Session session) {
        // tunnel-established -> BACKEND/FRONTEND-CLOSED -> tunnel-closing(close other session) -> tunnel-closed
        if(getState().equals(new TunnelClosing(this))) {
            setState(new TunnelClosed(this));
            return;
        }
        Session peer = null;
        switch (session.getSessionType()) {
            case FRONTEND:
                peer = backend;
                setState(new FrontendClosed(this));
                break;
            case BACKEND:
                peer = frontend;
                setState(new BackendClosed(this));
                break;

            default:
                logger.error("[handle] session type un-defined: {}", session.toString());
                return;
        }
        setState(new TunnelClosing(this));
        peer.release();
    }

    private void onSessionEstablished(Session session) {
        if(!(getState() instanceof TunnelHalfEstablished)) {
            logger.info("[doHandle] tunnel state {}, not able transfer to established", getState().name());
            return;
        }
        if(session.getSessionType() == SESSION_TYPE.BACKEND) {
            setState(new TunnelEstablished(this));
        }
    }

    @Override
    public int hashCode() {
       return identity.hashCode();
    }

    /**
     * Lifecycle corresponding*/
    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

        frontend = new DefaultFrontendSession(this, frontendChannel, config.getTrafficReportIntervalMillis());
        ProxyEndpointSelector selector = proxyResourceManager.createProxyEndpointSelector(protocol);
        // share the nio event loop to avoid oom
        backend = new DefaultBackendSession(this, frontend.getChannel().eventLoop(),
                config.getTrafficReportIntervalMillis(), selector);

        registerSessionEventHandlers();
        frontend.addObserver(this);
        backend.addObserver(this);

        LifecycleHelper.initializeIfPossible(frontend);
        LifecycleHelper.initializeIfPossible(backend);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LifecycleHelper.startIfPossible(frontend);
        LifecycleHelper.startIfPossible(backend);
    }

    private void registerSessionEventHandlers() {
        frontend.addSessionEventHandler(new FrontendSessionEventHandler());
        backend.addSessionEventHandler(new BackendSessionEventHandler());
    }

    @Override
    protected void doStop() throws Exception {
        release();
        super.doStop();
    }

    @Override
    public void release() throws Exception {
        if(getState().equals(new TunnelClosed(this))) {
            logger.debug("already closed, no need to release again");
            return;
        }
        setState(new TunnelClosed(this));
        if(frontend != null) {
            frontend.release();
        }
        if(backend != null) {
            backend.release();
        }
    }


    protected Channel frontendChannel() {
        return frontendChannel;
    }

    @VisibleForTesting
    protected void setFrontend(FrontendSession frontend) {
        this.frontend = frontend;
    }

    @VisibleForTesting
    protected void setBackend(BackendSession backend) {
        this.backend = backend;
    }

    class BackendSessionEventHandler implements SessionEventHandler {
        @Override
        public void onInit() {

        }

        @Override
        public void onEstablished() {
            frontend.makeReadable();
        }

        @Override
        public void onWritable() {
            logger.info("[onWritable][Backend][{}]open frontend auto read", identity());
            frontend.makeReadable();
            frontend.markReadLoggability(false);
            backend.markWriteLoggability(false);
        }

        @Override
        public void onNotWritable() {
            logger.info("[onNotWritable][Backend][{}]close frontend auto read", identity());
            frontend.markReadLoggability(true);
            backend.markWriteLoggability(true);
            frontend.makeUnReadable();
        }
    }

    class FrontendSessionEventHandler implements SessionEventHandler {
        @Override
        public void onInit() {
            frontend.makeUnReadable();
            frontend.getChannel().pipeline()
                    .addLast(new TunnelTrafficReporter(config.getTrafficReportIntervalMillis(), frontend));
        }

        @Override
        public void onEstablished() {

        }

        @Override
        public void onWritable() {
            logger.info("[onWritable][Frontend][{}]open backend auto read", identity());
            backend.makeReadable();
            backend.markReadLoggability(false);
            frontend.markWriteLoggability(false);
        }

        @Override
        public void onNotWritable() {
            logger.info("[onNotWritable][Frontend][{}]close backend auto read", identity());
            backend.markReadLoggability(true);
            frontend.markWriteLoggability(true);
            backend.makeUnReadable();
        }
    }
}
