package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.redis.core.config.TLSConfig;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class DefaultTlsConfig extends AbstractConfigBean implements TLSConfig {

    public DefaultTlsConfig() {
        setConfig(new DefaultFileConfig());
    }

    @Override
    public String getPassword() {
        return getProperty(KEY_CERT_PASSWORD, FoundationService.DEFAULT.getAppId());
    }

    @Override
    public String getServerCertFilePath() {
        return getProperty(KEY_SERVER_CERT_FILE_PATH, "/opt/cert/xpipe_server.jks");
    }

    @Override
    public String getClientCertFilePath() {
        return getProperty(KEY_CLIENT_CERT_FILE_PATH, "/opt/cert/xpipe_client.jks");
    }

    @Override
    public String getCertFileType() {
        return getProperty(KEY_CERT_FILE_TYPE, "JKS");
    }
}
