package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.redis.core.config.TLSConfig;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class DefaultTlsConfig extends AbstractConfigBean implements TLSConfig {

    @Override
    public String getServerCertChainFilePath() {
        return getProperty(KEY_SERVER_CERT_CHAIN_FILE_PATH, "/opt/cert/server.crt");
    }

    @Override
    public String getClientCertChainFilePath() {
        return getProperty(KEY_CLIENT_CERT_CHAIN_FILE_PATH, "/opt/cert/client.crt");
    }

    @Override
    public String getServerKeyFilePath() {
        return getProperty(KEY_SERVER_KEY_FILE_PATH, "/opt/cert/pkcs8_server.key");
    }

    @Override
    public String getClientKeyFilePath() {
        return getProperty(KEY_CLIENT_KEY_FILE_PATH, "/opt/cert/pkcs8_client.key");
    }

    @Override
    public String getRootFilePath() {
        return getProperty(KEY_ROOT_FILE_PATH, "/opt/cert/ca.crt");
    }

}
