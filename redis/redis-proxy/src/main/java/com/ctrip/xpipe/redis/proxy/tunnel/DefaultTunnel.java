package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.session.*;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.event.SessionClosedEventHandler;
import com.ctrip.xpipe.redis.proxy.event.SessionClosingEventHandler;
import com.ctrip.xpipe.redis.proxy.event.SessionEstablishedHandler;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelHalfEstablished;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultTunnel extends AbstractLifecycleObservable implements Tunnel {

    private static final String STATE_CHANGE = "Tunnel.State.Change";

    private static final Logger logger = LoggerFactory.getLogger(DefaultTunnel.class);

    private final String identity = UUID.randomUUID().toString();

    private Channel frontendChannel;

    private volatile FrontendSession frontend;

    private volatile BackendSession backend;

    private ProxyProtocol protocol;

    private ProxyEndpointManager endpointManager;

    private AtomicReference<TunnelState> tunnelState = new AtomicReference<>(new TunnelHalfEstablished(this));

    private NettySslHandlerFactory clientSslFactory;

    private ProxyConfig config;

    private AtomicBoolean isProxyProtocolSent = new AtomicBoolean(false);

    private EventLoopGroup backendEventLoopGroup;

    public DefaultTunnel(Channel frontendChannel, ProxyEndpointManager endpointManager, ProxyProtocol protocol,
                         NettySslHandlerFactory clientSslFactory, ProxyConfig config, EventLoopGroup eventLoopGroup) {

        this.config = config;
        this.protocol = protocol;
        this.frontendChannel = frontendChannel;
        this.endpointManager = endpointManager;
        this.clientSslFactory = clientSslFactory;
        this.backendEventLoopGroup = eventLoopGroup;
        initAndStart();
    }

    private void initAndStart() {
        try {
            LifecycleHelper.initializeIfPossible(this);
        } catch (Exception e) {
            logger.error("[initAndStart][init] ", e);
        }
        try {
            LifecycleHelper.startIfPossible(this);
        } catch (Exception e) {
            logger.error("[initAndStart][start] ", e);
        }
    }

    @Override
    public String identity() {
        return this.identity;
    }

    @Override
    public ChannelFuture forwardToBackend(ByteBuf message) {
        return backend().tryWrite(message);
    }

    @Override
    public ChannelFuture forwardToFrontend(ByteBuf message) {
        return frontend().tryWrite(message);
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

    @Override
    public synchronized void setState(TunnelState newState) {
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
            new SessionClosedEventHandler(session, event).handle();
        } else if(event.getCurrent() instanceof SessionClosing) {
            new SessionClosingEventHandler(session, event).handle();
        } else if(event.getCurrent() instanceof SessionEstablished) {
            new SessionEstablishedHandler(session, event).handle();
        } else {
            logger.info("[update] un-recognised event: {}", event);
        }
    }


    /**
     * Lifecycle corresponding*/
    @Override
    protected void doInitialize() throws Exception {
        frontend = new DefaultFrontendSession(this, frontendChannel(), config.getTrafficReportIntervalMillis());
        ProxyEndpointSelector selector = new DefaultProxyEndpointSelector(protocol.nextEndpoints(), endpointManager);
        selector.setNextHopAlgorithm(new NaiveNextHopAlgorithm());
        backend = new DefaultBackendSession(this, config.getTrafficReportIntervalMillis(), selector,
                backendEventLoopGroup, clientSslFactory);

        registerSessionEventHandlers();
        frontend.addObserver(this);
        backend.addObserver(this);

        LifecycleHelper.initializeIfPossible(frontend);
        LifecycleHelper.initializeIfPossible(backend);

        super.doInitialize();
    }

    @Override
    protected void doStart() throws Exception {
        LifecycleHelper.startIfPossible(frontend);
        LifecycleHelper.startIfPossible(backend);

        super.doStart();
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
        if(frontend != null && frontend.isReleasable()) {
            frontend.release();
        }
        if(backend != null && backend.isReleasable()) {
            backend.release();
        }
        if(endpointManager != null) {
            endpointManager.stop();
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
            frontend.makeReadable();
        }

        @Override
        public void onNotWritable() {
            frontend.makeUnReadable();
        }
    }

    class FrontendSessionEventHandler implements SessionEventHandler {
        @Override
        public void onInit() {
            frontend.makeUnReadable();
        }

        @Override
        public void onEstablished() {

        }

        @Override
        public void onWritable() {
            backend.makeReadable();
        }

        @Override
        public void onNotWritable() {
            backend.makeUnReadable();
        }
    }
}
