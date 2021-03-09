package com.ctrip.xpipe.redis.checker.alert;

import com.ctrip.xpipe.api.email.EmailResponse;

/**
 * @author lishanglin
 * date 2021/3/9
 */
public interface AlertEventRecorder {

    void record(AlertMessageEntity message, EmailResponse response);

}
