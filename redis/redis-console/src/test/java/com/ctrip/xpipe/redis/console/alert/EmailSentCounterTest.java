package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.model.EventModel;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public class EmailSentCounterTest {

    @Mock
    private AlertEventService service;

    @InjectMocks
    private EmailSentCounter instance = new EmailSentCounter();

    @Before
    public void beforeEmailSentCounterTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testStatistics() throws Exception {
        Properties properties1 = new Properties();
        properties1.put(EmailResponse.KEYS.CHECK_INFO.name(), "123456");
        String property1 = JsonCodec.DEFAULT.encode(properties1);
        EventModel event1 = new EventModel().setEventProperty(property1);

        Properties properties2 = new Properties();
        properties1.put(EmailResponse.KEYS.CHECK_INFO.name(), "1234567");
        String property2 = JsonCodec.DEFAULT.encode(properties2);
        EventModel event2 = new EventModel().setEventProperty(property2);

        when(service.getLastHourAlertEvent()).thenReturn(Lists.newArrayList(event1, event2));

        Pair<Integer, Integer> successAndFails = instance.statistics(service.getLastHourAlertEvent());
        Assert.assertEquals(2, (int)successAndFails.getKey());
        Assert.assertEquals(0, (int)successAndFails.getValue());
    }

    @Ignore
    @Test
    public void manualTestScheduled() throws InterruptedException {
        int count = 0;
        while(count++ < 66) {
            if ((count & 1) == 0) {
                instance.success();
            } else {
                instance.fail(new Exception());
            }
            Thread.sleep(1000);
        }

    }

    @Test
    public void testGetStartTime() throws Exception {
        long minute = instance.getStartTime();
        Date date = DateTimeUtils.getMinutesLaterThan(new Date(), (int)minute);
        Date expected = DateTimeUtils.getNearestHour();
        System.out.println(minute);
        System.out.println(date);
        System.out.println(expected);
        Assert.assertTrue(Math.abs(expected.getTime() - date.getTime()) <= TimeUnit.MINUTES.toMillis(1));
    }

}