package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.EventDao;
import com.ctrip.xpipe.redis.console.model.EventModel;
import com.ctrip.xpipe.redis.console.model.EventTbl;
import com.ctrip.xpipe.redis.console.service.AlertMailEventService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.function.Function;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */

@Service
public class AlertEventService extends AbstractEventService implements AlertMailEventService {

    @Autowired
    private EventDao eventDao;

    @Override
    public void insert(EventModel eventModel) {
        EventTbl eventTbl = eventModel.toEventTbl(new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s;
            }
        });
        if(eventTbl.getDataChangeLastTime() == null) {
            eventTbl.setDataChangeLastTime(new Date());
        }
        eventDao.insert(eventTbl);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<EventModel> findEventsByTypeAndDate(EventModel.EventType eventType, Date date) {
        List<EventTbl> eventTbls = eventDao.findEventsByTypeAndDate(eventType.name(), date);
        if(eventTbls == null || eventTbls.isEmpty()) {
            return Lists.newLinkedList();
        }
        return transferToEventModels(eventTbls, new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s;
            }
        });
    }

    @Override
    public List<EventModel> getLastHourAlertEvent() {
        return findEventsByTypeAndDate(EventModel.EventType.ALERT_EMAIL,
                DateTimeUtils.getHoursBeforeDate(new Date(), 1));
    }
}
