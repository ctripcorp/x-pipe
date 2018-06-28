package com.ctrip.xpipe.redis.core.proxy.handler;

import com.ctrip.xpipe.redis.core.config.TLSConfig;

/**
 * @author chen.zhu
 * <p>
 * May 25, 2018
 */
public class FakeTLSConfig implements TLSConfig {

    @Override
    public String getPassword() {
        return "123456";
    }

    @Override
    public String getServerCertFilePath() {
        return "/opt/cert/xpipe-server.jks";
    }

    @Override
    public String getClientCertFilePath() {
        return "/opt/cert/xpipe-client.jks";
    }

    @Override
    public String getCertFileType() {
        return "JKS";
    }

}
