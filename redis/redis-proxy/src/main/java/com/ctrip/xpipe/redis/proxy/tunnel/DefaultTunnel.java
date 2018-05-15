package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.proxy.exception.WriteWhenSessionInitException;
import com.ctrip.xpipe.redis.proxy.handler.TunnelTrafficReporter;
import com.ctrip.xpipe.redis.proxy.model.TunnelMeta;
import com.ctrip.xpipe.redis.proxy.session.DefaultSession;
import com.ctrip.xpipe.redis.proxy.session.DefaultSessionStore;
import com.ctrip.xpipe.redis.proxy.session.SessionStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.session.SessionStore;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import com.ctrip.xpipe.redis.proxy.tunnel.event.SessionClosedEventHandler;
import com.ctrip.xpipe.redis.proxy.tunnel.event.SessionClosingEventHandler;
import com.ctrip.xpipe.redis.proxy.tunnel.event.SessionEstablishedHandler;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosed;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelHalfEstablished;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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

    private ProxyProtocol protocol;

    private SessionStore sessionStore;

    private ProxyEndpointManager endpointManager;

    private AtomicReference<TunnelState> tunnelState = new AtomicReference<>(new TunnelHalfEstablished(this));

    private NettySslHandlerFactory clientSslFactory;

    private ProxyConfig config;

    private AtomicBoolean isProxyProtocolSent = new AtomicBoolean(false);

    private EventLoopGroup nioEventLoopGroup;

    public DefaultTunnel(Channel frontendChannel, ProxyEndpointManager endpointManager, ProxyProtocol protocol,
                         NettySslHandlerFactory clientSslFactory, ProxyConfig config) {
        this.endpointManager = endpointManager;
        this.frontendChannel = frontendChannel;
        this.protocol = protocol;
        this.clientSslFactory = clientSslFactory;
        this.config = config;
        this.nioEventLoopGroup = new NioEventLoopGroup(config.backendEventLoopNum(),
                XpipeThreadFactory.create(String.format("backend-%s", identity())));
        this.sessionStore = new DefaultSessionStore(this, nioEventLoopGroup);
    }

    @Override
    public String identity() {
        return this.identity;
    }

    @Override
    public void sendProxyProtocol() {
        if(isProxyProtocolSent.compareAndSet(false, true)) {
            backend().tryWrite(protocol.output())
                    .addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                isProxyProtocolSent.set(false);
                                future.channel().eventLoop().schedule(new Runnable() {
                                    @Override
                                    public void run() {
                                        sendProxyProtocol();
                                    }
                                }, 5, TimeUnit.MILLISECONDS);
                            } else {
                                // unblock read event, since backend functionality is ready
                                frontendChannel().config().setAutoRead(true);
                            }
                        }
                    });
        }
    }

    @Override
    public Session frontend() {
        return sessionStore.frontend();
    }

    @Override
    public Session backend() {
        return sessionStore.backend();
    }

    @Override
    public Session session(Channel channel) {
        Session session = sessionStore.session(channel);
        if(session == null) {
            throw new ResourceNotFoundException("Session not found for channel");
        }
        return session;
    }

    @Override
    public TunnelMeta getTunnelMeta() {
        return new TunnelMeta(this, protocol);
    }

    @Override
    public ProxyEndpoint getNextJump() {
        return endpointManager.getNextJump(protocol.nextEndpoints());
    }

    @Override
    public synchronized void setState(TunnelState newState) {
        if(!tunnelState.get().isValidNext(newState)) {
            logger.error("[setState] Set state failed, state relationship not match, old: {}, new: {}",
                    tunnelState.get(), newState);
            return;
        }
        TunnelState oldState = this.tunnelState.getAndSet(newState);
        if (!oldState.equals(newState)) {
            logger.info("[setState] tunnel state change: {} -> {} ({})", oldState, newState, getTunnelMeta());
            EventMonitor.DEFAULT.logEvent(STATE_CHANGE, String.format("Tunnel state change: %s -> %s", oldState, newState));
            notifyObservers(new TunnelStateChangeEvent(oldState, newState));
        } else {
            logger.info("[setState] already the same state: {}", oldState);
        }

    }

    @Override
    public TunnelState getState() {
        return this.tunnelState.get();
    }

    @Override
    public void forward(ByteBuf message, Session src) {
        logger.debug("[forward] Forward message: {}", message.toString(Charset.defaultCharset()));
        getState().forward(message, src);
    }

    public void doForward(ByteBuf message, Session src) {

        try {
            message.retain();

            Session target = sessionStore.getOppositeSession(src);
            ChannelFuture future = target.tryWrite(message);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(!future.isSuccess()) {
                        logger.error("[doForward] write error: {}");
                        target.setSessionState(new SessionClosing((DefaultSession) target));
                    }
                }
            });
        } catch (WriteWhenSessionInitException e) {
            logger.info("[doForward][Backend not init]");
        } catch (Exception e) {
            logger.error("[doForward] error when forward message: ", e);
        }
    }

    @Override
    public void release() throws Exception {
        if(getState().equals(new TunnelClosed(this))) {
            logger.info("already closed, no need to release again");
            return;
        }
        sessionStore.release();
        if(nioEventLoopGroup != null) {
            nioEventLoopGroup.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS);
            nioEventLoopGroup = null;
        }
        if(endpointManager != null) {
            endpointManager.stop();
            endpointManager = null;
        }
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

    @Override
    protected void doInitialize() throws Exception {
        LifecycleHelper.initializeIfPossible(sessionStore);
        // add traffic report when tunnel establish
        frontendChannel().pipeline().addLast(new TunnelTrafficReporter(config.getTrafficReportIntervalMillis(), this));
        super.doInitialize();
    }

    @Override
    protected void doStart() throws Exception {
        LifecycleHelper.startIfPossible(sessionStore);
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        release();
        super.doStop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
    }

    public Channel frontendChannel() {
        return frontendChannel;
    }

    public long getTrafficReportMilli() {
        return config.getTrafficReportIntervalMillis();
    }

    public NettySslHandlerFactory sslHandlerFactory() {
        return clientSslFactory;
    }

    @VisibleForTesting
    public SessionStore getSessionStore() {
        return this.sessionStore;
    }
}
