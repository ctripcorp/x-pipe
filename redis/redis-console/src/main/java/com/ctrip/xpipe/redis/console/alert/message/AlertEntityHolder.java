package com.ctrip.xpipe.redis.console.alert.message;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;

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
