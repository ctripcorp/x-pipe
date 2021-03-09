package com.ctrip.xpipe.redis.checker.alert.recorder;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.redis.checker.alert.AlertEventRecorder;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/3/9
 */
@Component
public class CheckerAlertEventRecorder implements AlertEventRecorder {

    @Override
    public void record(AlertMessageEntity message, EmailResponse response) {
        // TODO: push log to Console periodically @sl_li
    }

    public static class AlertEventLog {

    }

}
