package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DefaultSessionStore extends AbstractLifecycle implements SessionStore {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSessionStore.class);

    private DefaultTunnel tunnel;

    private volatile Session frontend;

    private volatile Session backend;

    private EventLoopGroup nioEventLoopGroup;

    public DefaultSessionStore(DefaultTunnel tunnel, EventLoopGroup eventLoopGroup) {
        this.tunnel = tunnel;
        this.nioEventLoopGroup = eventLoopGroup;
    }

    @Override
    public Tunnel tunnel() {
        return tunnel;
    }

    @Override
    public Session session(Channel channel) {
        if(frontend.getChannel() == channel) {
            return frontend;
        }
        if(backend.getChannel() == channel) {
            return backend;
        }
        logger.error("[session] channel not matched, channel: {}", channel);
        return null;
    }

    @Override
    public Session frontend() {
        if(frontend == null) {
            synchronized (this) {
                if(frontend == null) {
                    frontend = new DefaultSession(tunnel, SESSION_TYPE.FRONTEND, tunnel.frontendChannel(),
                            tunnel.sslHandlerFactory(), tunnel.getTrafficReportMilli());
                    frontend.addObserver(tunnel);
                }
            }
        }
        return frontend;
    }

    @Override
    public Session backend() {
        if(backend == null) {
            synchronized (this) {
                if(backend == null) {
                    backend = new DefaultSession(tunnel, SESSION_TYPE.BACKEND, tunnel().getNextJump(),
                            tunnel.sslHandlerFactory(), tunnel.getTrafficReportMilli());
                    ((DefaultSession) backend).setNioEventLoopGroup(nioEventLoopGroup);
                    backend.addObserver(tunnel);
                }
            }
        }
        return backend;
    }

    @Override
    public Session getOppositeSession(Session src) {
        if(src == null) {
            throw new ResourceNotFoundException("null object from input");
        }
        if(src == frontend) {
            return backend();
        }
        if(src == backend) {
            return frontend();
        }
        logger.error("[getOppositeSession] not found for source: {}", src);
        throw new ResourceNotFoundException("Opposite Session is not found");
    }


    @Override
    public List<Session> getSessions() {
        return Lists.newArrayList(frontend, backend);
    }

    @Override
    public void release() {
        getSessions().forEach(session -> {
            try {
                logger.info("[release] session: {}", session.getSessionMeta());
                session.release();
            } catch (Exception e) {
                logger.error("[release] Exception when release session", e);
            }
        });
    }

    @Override
    protected void doInitialize() throws Exception {
        frontend();
        backend();
        super.doInitialize();
    }

    @Override
    protected void doStart() throws Exception {
        ChannelFuture future = backend().connect();
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if(future.isSuccess()) {
                    tunnel.sendProxyProtocol();
                }
            }
        });
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        release();
        super.doStop();
    }

}
