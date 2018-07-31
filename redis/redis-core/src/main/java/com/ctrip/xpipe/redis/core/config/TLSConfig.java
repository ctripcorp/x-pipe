package com.ctrip.xpipe.redis.core.config;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public interface TLSConfig {

    String getCertChainFilePath();

    String getKeyFilePath();

    String getRootFilePath();

    int getMaxPacketBufferSize();

    String KEY_CERT_CHAIN_FILE_PATH = "proxy.cert.chain.file.path";

    String KEY_KEY_FILE_PATH = "proxy.key.file.path";

    String KEY_ROOT_FILE_PATH = "proxy.root.file.path";

}
