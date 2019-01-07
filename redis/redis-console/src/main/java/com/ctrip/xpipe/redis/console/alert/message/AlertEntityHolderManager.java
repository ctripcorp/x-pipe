package com.ctrip.xpipe.redis.console.alert.message;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AlertEntityHolderManager {

    boolean hasAlertsToSend();

    List<AlertEntityHolder> allAlertsToSend();

    void holdAlert(AlertEntity alertEntity);

    void bulkInsert(List<AlertEntity> alertEntities);

    Map<ALERT_TYPE, Set<AlertEntity>> convertToMap();
}
