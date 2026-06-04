package com.ctrip.xpipe.service.sso;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.utils.EncryptUtils;

import static com.ctrip.xpipe.api.config.ConfigProvider.COMMON_CONFIG;

public class SsoControlConfig extends AbstractConfigBean {

    private static final String KEY_SSO_CONTROL_TOKEN = "sso.token";

    public SsoControlConfig() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(COMMON_CONFIG));
    }

    public String getSsoControlToken() {
        String rawToken = getProperty(KEY_SSO_CONTROL_TOKEN, "");
        try {
            return EncryptUtils.decryptAES_ECB(rawToken, FoundationService.DEFAULT.getAppId());
        } catch (Throwable th) {
            return rawToken;
        }
    }

}
