package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.EventModel;
import com.ctrip.xpipe.redis.console.model.EventTbl;
import com.ctrip.xpipe.redis.console.service.EventService;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.function.Function;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */

public abstract class AbstractEventService<T> implements EventService<T> {

    @SuppressWarnings("unchecked")
    protected <T> List<EventModel<T>> transferToEventModels(List<EventTbl> eventTblList, Function<String, T> function) {
        List<EventModel<T>> result = Lists.newLinkedList();
        for(EventTbl eventTbl : eventTblList) {
            result.add(new EventModel<T>().fromEventTbl(eventTbl, function));
        }
        return result;
    }

}
