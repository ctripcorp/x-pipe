package com.ctrip.xpipe.redis.core.config;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public interface TLSConfig {

    String getServerCertChainFilePath();

    String getClientCertChainFilePath();

    String getServerKeyFilePath();

    String getClientKeyFilePath();

    String getRootFilePath();

    String KEY_SERVER_CERT_CHAIN_FILE_PATH = "proxy.server.cert.chain.file.path";

    String KEY_CLIENT_CERT_CHAIN_FILE_PATH = "proxy.client.cert.chain.file.path";

    String KEY_SERVER_KEY_FILE_PATH = "proxy.server.key.file.path";

    String KEY_CLIENT_KEY_FILE_PATH = "proxy.client.key.file.path";

    String KEY_ROOT_FILE_PATH = "proxy.root.file.path";

}
