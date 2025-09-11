package com.ctrip.xpipe.redis.checker.alert.message.holder;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class DefaultAlertEntityHolder implements AlertEntityHolder {

    private ALERT_TYPE alertType;

    private Set<AlertEntity> alerts = Sets.newConcurrentHashSet();

    public DefaultAlertEntityHolder(ALERT_TYPE alertType) {
        this.alertType = alertType;
    }

    @Override
    public ALERT_TYPE getAlertType() {
        return alertType;
    }

    @Override
    public synchronized void hold(AlertEntity alertEntity) {
        if(!alertEntity.getAlertType().equals(alertType)) {
            throw new IllegalArgumentException(String.format("need alert type: %s, but receive: %s",
                    alertType.name(), alertEntity.getAlertType().name()));
        }
        // refresh to remove first
        alerts.remove(alertEntity);
        alerts.add(alertEntity);
    }

    @Override
    public boolean remove(AlertEntity alertEntity) {
        return alerts.remove(alertEntity);
    }

    @Override
    public synchronized void removeIf(Predicate<AlertEntity> predicate) {
        alerts.removeIf(predicate);
    }

    @Override
    public synchronized boolean hasAlerts() {
        return !alerts.isEmpty();
    }

    @Override
    public List<AlertEntity> allAlerts() {
        return Lists.newArrayList(alerts);
    }

    @Override
    public String toString() {
        return String.format("DefaultAlertEntityHolder{alertType: %s, alert size: %s", alertType.name(), alerts.size());
    }
}
