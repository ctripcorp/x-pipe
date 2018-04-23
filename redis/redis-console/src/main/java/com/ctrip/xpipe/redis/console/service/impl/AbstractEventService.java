package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.EventDao;
import com.ctrip.xpipe.redis.console.model.EventModel;
import com.ctrip.xpipe.redis.console.model.EventTbl;
import com.ctrip.xpipe.redis.console.service.EventService;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.function.Function;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */

public abstract class AbstractEventService<T> implements EventService<T> {

    @Autowired
    private EventDao eventDao;

    @SuppressWarnings("unchecked")
    protected <T> List<EventModel<T>> transferToEventModels(List<EventTbl> eventTblList, Function<String, T> function) {
        List<EventModel<T>> result = Lists.newLinkedList();
        for(EventTbl eventTbl : eventTblList) {
            result.add(new EventModel<T>().fromEventTbl(eventTbl, function));
        }
        return result;
    }

}
