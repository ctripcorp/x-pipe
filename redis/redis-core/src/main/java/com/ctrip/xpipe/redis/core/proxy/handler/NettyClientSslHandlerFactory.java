package com.ctrip.xpipe.redis.core.proxy.handler;

import com.ctrip.xpipe.redis.core.config.TLSConfig;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

import java.io.File;

/**
 * @author chen.zhu
 * <p>
 * May 11, 2018
 */
public class NettyClientSslHandlerFactory extends AbstractNettySslHandlerFactory {

    private volatile SslContext sslContext;

    public NettyClientSslHandlerFactory(TLSConfig config) {
        super(config);
    }

    @Override
    public SslHandler createSslHandler(SocketChannel channel) {
        SslHandler sslHandler = getNettySslContext().newHandler(channel.alloc());
        return getCustomizedSslHandler(sslHandler);
    }

    private SslContext getNettySslContext() {
        if(sslContext == null) {
            synchronized (this) {
                if(sslContext == null) {
                    try {
                        File certChainFile = new File(tlsConfig.getCertChainFilePath());
                        File keyFile = new File(tlsConfig.getKeyFilePath());
                        File rootFile = new File(tlsConfig.getRootFilePath());

                        sslContext = SslContextBuilder.forClient()
                                .keyManager(certChainFile, keyFile)
                                .trustManager(rootFile)
                                .sslProvider(SslProvider.OPENSSL).build();
                    } catch (Exception e) {
                        logger.error("[getNettySslContext] ", e);
                    }
                }
            }
        }
        return sslContext;
    }

}
