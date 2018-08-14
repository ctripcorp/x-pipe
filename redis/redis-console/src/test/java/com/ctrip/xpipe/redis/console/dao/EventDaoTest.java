package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.model.EventModel;
import com.ctrip.xpipe.redis.console.model.EventTbl;
import com.ctrip.xpipe.redis.console.model.EventTblEntity;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */
public class EventDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private EventDao eventDao;

    @Override
    protected String prepareDatas() throws IOException {
        // empty the database
        String sql = prepareDatasFromFile("src/main/resources/sql/h2/xpipedemodbtables.sql");
        return sql;
    }

    @Test
    public void testInsert() {
        eventDao.insert(new EventTbl().setEventDetail("test").setEventType(EventModel.EventType.ALERT_EMAIL.name())
                .setEventOperation(ALERT_TYPE.CLIENT_INCONSIS.name()).setEventProperty("12345").setEventOperator("none"));
    }

    @Test
    public void findEventsByTypeAndDate() throws DalException {
        Date date = new Date((System.currentTimeMillis() - 1000 * 60 * 60));
        eventDao.insert(new EventTbl().setEventDetail("test").setEventType(EventModel.EventType.ALERT_EMAIL.name())
                .setEventOperation(ALERT_TYPE.CLIENT_INCONSIS.name()).setEventProperty("12345").setEventOperator("none")
                .setDataChangeLastTime(new Date()));
        List<EventTbl> eventDetails = eventDao.eventTblDao.findEventByType(EventModel.EventType.ALERT_EMAIL.name(), EventTblEntity.READSET_FULL);
        logger.info("{}", eventDetails);
        logger.info("{}", date);
        List<EventTbl> events = eventDao.findEventsByTypeAndDate(EventModel.EventType.ALERT_EMAIL.name(), date);
        logger.info("{}", events);
        Assert.assertFalse(events.isEmpty());
    }
}