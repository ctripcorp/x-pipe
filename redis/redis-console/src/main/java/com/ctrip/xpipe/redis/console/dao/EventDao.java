package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.EventTbl;
import com.ctrip.xpipe.redis.console.model.EventTblDao;
import com.ctrip.xpipe.redis.console.model.EventTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import jakarta.annotation.PostConstruct;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import java.util.Date;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */

@Repository
public class EventDao extends AbstractXpipeConsoleDAO {

    protected EventTblDao eventTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            eventTblDao = ContainerLoader.getDefaultContainer().lookup(EventTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.", e);
        }
    }

    public void insert(EventTbl eventTbl) {
        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return eventTblDao.insert(eventTbl);
            }
        });
    }

    public List<EventTbl> findEventsByTypeAndDate(String eventType, Date date) {
        return queryHandler.handleQuery(new DalQuery<List<EventTbl>>() {
            @Override
            public List<EventTbl> doQuery() throws DalException {
                return eventTblDao.findEventByTypeAfterDate(eventType, date, EventTblEntity.READSET_FULL);
            }
        });
    }
}
