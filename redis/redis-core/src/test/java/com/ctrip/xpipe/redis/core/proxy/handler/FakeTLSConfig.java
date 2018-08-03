package com.ctrip.xpipe.redis.core.proxy.handler;

import com.ctrip.xpipe.redis.core.config.TLSConfig;

/**
 * @author chen.zhu
 * <p>
 * May 25, 2018
 */
public class FakeTLSConfig implements TLSConfig {

    @Override
    public String getServerCertChainFilePath() {
        return "/opt/cert/server.crt";
    }

    @Override
    public String getClientCertChainFilePath() {
        return "/opt/cert/client.crt";
    }

    @Override
    public String getServerKeyFilePath() {
        return "/opt/cert/pkcs8_server.key";
    }

    @Override
    public String getClientKeyFilePath() {
        return "/opt/cert/pkcs8_client.key";
    }

    @Override
    public String getRootFilePath() {
        return "/opt/cert/ca.crt";
    }

}
