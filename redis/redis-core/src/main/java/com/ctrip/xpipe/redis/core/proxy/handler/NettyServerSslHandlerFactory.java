package com.ctrip.xpipe.redis.core.proxy.handler;

import com.ctrip.xpipe.redis.core.config.TLSConfig;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.security.KeyStore;

/**
 * @author chen.zhu
 * <p>
 * May 11, 2018
 */
public class NettyServerSslHandlerFactory extends AbstractNettySslHandlerFactory {

    private volatile SSLContext sslContext;

    public NettyServerSslHandlerFactory(TLSConfig config) {
        super(config);
    }

    @Override
    protected String getFilePath() {
        return tlsConfig.getServerCertFilePath();
    }

    @Override
    public ChannelHandler createSslHandler() {
        SSLContext sslContext = initSSLContext();
        if(sslContext == null) {
            return null;
        }
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(false);

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
                        KeyManagerFactory keyManagerFactory = KeyManagerFactory
                                .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        keyManagerFactory.init(keyStore, getPassword().toCharArray());
                        sslContext = SSLContext.getInstance(SSL_TYPE);

                        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
                    } catch (Exception e) {
                        logger.error("[initSSLContext] ", e);
                    }
                }
            }
        }
        return sslContext;
    }
}
