package com.ctrip.xpipe.redis.checker.alert.message;

import com.ctrip.xpipe.redis.checker.alert.AlertEntity;

/**
 * @author qifanwang
 * <p>
 * July 1, 2024
 */

public interface DelayAlertRecoverySubscriber {
    // 延迟告警初次加入入口
    void addDelayAlerts(AlertEntity alert);
    // 处理延迟告警类型
    void doProcessDelayAlerts(AlertEntity alert);
}
