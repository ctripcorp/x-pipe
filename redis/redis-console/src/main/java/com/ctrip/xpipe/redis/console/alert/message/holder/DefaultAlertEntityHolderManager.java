package com.ctrip.xpipe.redis.console.alert.message.holder;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.message.AlertEntityHolder;
import com.ctrip.xpipe.redis.console.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class DefaultAlertEntityHolderManager implements AlertEntityHolderManager {

    private ConcurrentMap<ALERT_TYPE, AlertEntityHolder> holders = Maps.newConcurrentMap();

    @Override
    public synchronized boolean hasAlertsToSend() {
        return !holders.isEmpty();
    }

    @Override
    public List<AlertEntityHolder> allAlertsToSend() {
        List<AlertEntityHolder> result = Lists.newArrayList();
        synchronized (this) {
            for(AlertEntityHolder holder : holders.values()) {
                if(holder.hasAlerts()) {
                    result.add(holder);
                }
            }
        }
        return result;
    }

    @Override
    public void holdAlert(AlertEntity alertEntity) {
        AlertEntityHolder holder = MapUtils.getOrCreate(holders, alertEntity.getAlertType(), new ObjectFactory<AlertEntityHolder>() {
            @Override
            public AlertEntityHolder create() {
                return new DefaultAlertEntityHolder(alertEntity.getAlertType());
            }
        });
        holder.hold(alertEntity);
    }


    @Override
    public void bulkInsert(List<AlertEntity> alertEntities) {
        AlertEntity alertEntity = alertEntities.get(0);
        AlertEntityHolder holder = MapUtils.getOrCreate(holders, alertEntity.getAlertType(), new ObjectFactory<AlertEntityHolder>() {
            @Override
            public AlertEntityHolder create() {
                return new DefaultAlertEntityHolder(alertEntity.getAlertType());
            }
        });
        for(AlertEntity entity : alertEntities) {
            holder.hold(entity);
        }
    }

    @Override
    public Map<ALERT_TYPE, Set<AlertEntity>> convertToMap() {
        if(!hasAlertsToSend()) {
            return Maps.newHashMap();
        }
        Map<ALERT_TYPE, Set<AlertEntity>> result = Maps.newHashMap();
        for(Map.Entry<ALERT_TYPE, AlertEntityHolder> entry : holders.entrySet()) {
            result.put(entry.getKey(), Sets.newHashSet(entry.getValue().allAlerts()));
        }
        return result;
    }
}
