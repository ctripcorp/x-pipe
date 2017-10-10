package com.ctrip.xpipe.api.email;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author chen.zhu
 *
 * Oct 09, 2017
 */

public interface EmailService extends Ordered {

    EmailService DEFAULT = ServicesUtil.getEmailService();

    <T extends Email> void sendEmail(T t, Object... context);
}
