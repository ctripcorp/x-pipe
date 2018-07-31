package com.ctrip.xpipe.redis.core.proxy.handler;

import com.ctrip.xpipe.redis.core.config.TLSConfig;

/**
 * @author chen.zhu
 * <p>
 * May 25, 2018
 */
public class FakeTLSConfig implements TLSConfig {

    @Override
    public String getCertChainFilePath() {
        return "/opt/cert/xpipe.crt";
    }

    @Override
    public String getKeyFilePath() {
        return "/opt/cert/xpipe.key";
    }

    @Override
    public String getRootFilePath() {
        return "/opt/cert/ca.crt";
    }

    @Override
    public int getMaxPacketBufferSize() {
        return 2048;
    }

}
