package com.ctrip.xpipe.redis.checker.alert.message;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;

import java.util.List;
import java.util.function.Predicate;

public interface AlertEntityHolder {

    ALERT_TYPE getAlertType();

    void hold(AlertEntity alertEntity);

    boolean remove(AlertEntity alertEntity);

    void removeIf(Predicate<AlertEntity> predicate);

    boolean hasAlerts();

    List<AlertEntity> allAlerts();
}
