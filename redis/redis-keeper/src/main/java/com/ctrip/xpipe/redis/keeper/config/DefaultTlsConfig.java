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
    public String getCertChainFilePath() {
        return getProperty(KEY_CERT_CHAIN_FILE_PATH, "/opt/cert/xpipe.crt");
    }

    @Override
    public String getKeyFilePath() {
        return getProperty(KEY_KEY_FILE_PATH, "/opt/cert/xpipe.key");
    }

    @Override
    public String getRootFilePath() {
        return getProperty(KEY_ROOT_FILE_PATH, "/opt/cert/ca.crt");
    }

    @Override
    public int getMaxPacketBufferSize() {
        return 2048;
    }
}
