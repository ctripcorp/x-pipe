package com.ctrip.xpipe.redis.core.proxy.handler;

import com.ctrip.xpipe.redis.core.config.TLSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

/**
 * @author chen.zhu
 * <p>
 * May 11, 2018
 */
public abstract class AbstractNettySslHandlerFactory implements NettySslHandlerFactory {

    protected static Logger logger = LoggerFactory.getLogger(AbstractNettySslHandlerFactory.class);

    protected static final String SSL_TYPE = "TLS";

    protected TLSConfig tlsConfig;

    public AbstractNettySslHandlerFactory(TLSConfig tlsConfig) {
        this.tlsConfig = tlsConfig;
    }

    protected KeyStore loadKeyStore() {
        KeyStore keyStore = null;

        try {
            keyStore = KeyStore.getInstance(tlsConfig.getCertFileType());
            InputStream inputStream = new FileInputStream(getFilePath());
            keyStore.load(inputStream, getPassword().toCharArray());
        } catch (Exception e) {
            logger.error("[loadKeyStore] {}", e);
        }

        return keyStore;
    }

    protected abstract String getFilePath();

    protected String getPassword() {
        return tlsConfig.getPassword();
    }
}
