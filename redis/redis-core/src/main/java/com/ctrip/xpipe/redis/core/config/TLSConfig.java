package com.ctrip.xpipe.redis.core.config;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public interface TLSConfig {

    String getPassword();

    String getServerCertFilePath();

    String getClientCertFilePath();

    String getCertFileType();

    String KEY_SERVER_CERT_FILE_PATH = "proxy.server.cert.file.path";

    String KEY_CLIENT_CERT_FILE_PATH = "proxy.client.cert.file.path";

    String KEY_CERT_FILE_TYPE = "proxy.cert.file.type";

    String KEY_CERT_PASSWORD = "proxy.cert.password";

}
