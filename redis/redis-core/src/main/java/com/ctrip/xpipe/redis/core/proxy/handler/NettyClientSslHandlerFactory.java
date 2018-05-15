package com.ctrip.xpipe.redis.core.proxy.handler;

import com.ctrip.xpipe.redis.core.config.TLSConfig;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;

/**
 * @author chen.zhu
 * <p>
 * May 11, 2018
 */
public class NettyClientSslHandlerFactory extends AbstractNettySslHandlerFactory {

    private volatile SSLContext sslContext;

    public NettyClientSslHandlerFactory(TLSConfig config) {
        super(config);
    }

    @Override
    protected String getFilePath() {
        return tlsConfig.getClientCertFilePath();
    }

    @Override
    public ChannelHandler createSslHandler() {
        SSLContext sslContext = initSSLContext();
        if(sslContext == null) {
            return null;
        }

        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        return new SslHandler(sslEngine);
    }

    private SSLContext initSSLContext() {
        if(sslContext == null) {
            synchronized (this) {
                if(sslContext == null) {
                    KeyStore keyStore = loadKeyStore();
                    if (keyStore == null) {
                        return null;
                    }

                    try {
                        TrustManagerFactory trustManagerFactory = TrustManagerFactory
                                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                        trustManagerFactory.init(keyStore);

                        sslContext = SSLContext.getInstance(SSL_TYPE);
                        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
                    } catch (Exception e) {
                        logger.error("[initSSLContext] ", e);
                    }
                }
            }
        }
        return sslContext;
    }
}
