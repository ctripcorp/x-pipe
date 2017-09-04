package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.config.AbstractConfigBean;


/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public class CtripOrganizationConfig extends AbstractConfigBean {

    public static final String KEY_CMS_ACCESS_TOKEN = "cms.access.token";
    public static final String KEY_CMS_ORGANIZATION_URL = "cms.organization.url";

    public String getCmsAccessToken() {
        return getProperty(KEY_CMS_ACCESS_TOKEN, "");
    }

    public String getCmsOrganizationUrl() {
        return getProperty(KEY_CMS_ORGANIZATION_URL, "");
    }

}
