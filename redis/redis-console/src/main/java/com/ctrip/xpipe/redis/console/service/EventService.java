package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.EventModel;

import java.util.Date;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */
public interface EventService<T> {

    void insert(EventModel<T> eventModel);

    List<EventModel<T>> findEventsByTypeAndDate(EventModel.EventType eventType, Date date);

}
