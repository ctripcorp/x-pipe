package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.EventModel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */
public interface AlertMailEventService extends EventService {
    List<EventModel> getLastHourAlertEvent();
}
