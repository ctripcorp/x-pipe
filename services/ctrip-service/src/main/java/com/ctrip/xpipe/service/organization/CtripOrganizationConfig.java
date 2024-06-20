package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.utils.EncryptUtils;

import static com.ctrip.xpipe.api.config.ConfigProvider.COMMON_CONFIG;


/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public class CtripOrganizationConfig extends AbstractConfigBean {

    public static final String KEY_CMS_ACCESS_TOKEN = "cms.access.token";
    public static final String KEY_CMS_ORGANIZATION_URL = "cms.organization.url";

    public CtripOrganizationConfig() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(COMMON_CONFIG));
    }

    public String getCmsAccessToken() {
        String rawToken = getProperty(KEY_CMS_ACCESS_TOKEN, "");
        try {
            return EncryptUtils.decryptAES_ECB(rawToken, FoundationService.DEFAULT.getAppId());
        } catch (Throwable th) {
            return rawToken;
        }
    }

    public String getCmsOrganizationUrl() {
        return getProperty(KEY_CMS_ORGANIZATION_URL, "");
    }

}
