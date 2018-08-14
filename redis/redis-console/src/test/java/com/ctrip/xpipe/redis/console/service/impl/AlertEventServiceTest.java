package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.model.EventModel;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */

@SuppressWarnings("unchecked")
public class AlertEventServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private AlertEventService service;

    @Test
    public void insert() {
        service.insert(new EventModel().setEventDetail("test"));
        service.insert(new EventModel().setEventType(EventModel.EventType.UNKNOWN).setEventDetail("test"));
        service.insert(new EventModel().setEventType(EventModel.EventType.UNKNOWN).setEventDetail("test")
                                        .setEventOperation(ALERT_TYPE.CLIENT_INCONSIS.name()));
        service.insert(new EventModel().setEventType(EventModel.EventType.UNKNOWN).setEventDetail("test")
                                        .setEventOperator("10.2.81.68"));
    }

    @Test
    public void findEventsByTypeAndDate() {
        Date date = new Date();
        service.insert(new EventModel().setEventType(EventModel.EventType.UNKNOWN).setEventDetail("test")
                .setEventOperation(ALERT_TYPE.CLIENT_INCONSIS.name()).setLastUpdate(new Date()));
        service.insert(new EventModel().setEventType(EventModel.EventType.ALERT_EMAIL).setEventDetail("test2")
                .setEventOperation(ALERT_TYPE.CLIENT_INCONSIS.name()).setLastUpdate(new Date()));
        List<EventModel> events = service.findEventsByTypeAndDate(EventModel.EventType.ALERT_EMAIL, date);
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(ALERT_TYPE.CLIENT_INCONSIS.name(), events.get(0).getEventOperation());


        events = service.findEventsByTypeAndDate(EventModel.EventType.ALERT_EMAIL, DateTimeUtils.getMinutesLaterThan(date, 120));
        Assert.assertTrue(events.isEmpty());
    }

    @Test
    public void testGetLastHourAlertEvent() {
        service.insert(new EventModel().setEventType(EventModel.EventType.ALERT_EMAIL).setEventDetail("test")
                .setEventOperation(ALERT_TYPE.CLIENT_INCONSIS.name()).setLastUpdate(DateTimeUtils.getHoursBeforeDate(new Date(), 2)));
        service.insert(new EventModel().setEventType(EventModel.EventType.ALERT_EMAIL).setEventDetail("test2")
                .setEventOperation(ALERT_TYPE.CLIENT_INCONSIS.name()));

        List<EventModel> events = service.getLastHourAlertEvent();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(ALERT_TYPE.CLIENT_INCONSIS.name(), events.get(0).getEventOperation());
    }
}