package com.ctrip.xpipe.api.email;

import java.util.Properties;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */
public interface EmailResponse {
    Properties getProperties();

    enum KEYS {
        CHECK_INFO
    }
}
