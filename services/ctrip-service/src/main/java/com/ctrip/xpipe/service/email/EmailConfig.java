package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.config.AbstractConfigBean;

public class EmailConfig extends AbstractConfigBean {

    public static final String KEY_EMAIL_SERVICE_URL = "email.service.url";

    public String getEmailServiceUrl() {
        return getProperty(KEY_EMAIL_SERVICE_URL, "");
    }

}
