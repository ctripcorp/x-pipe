package com.ctrip.xpipe.redis.console.model;

import java.util.Date;
import java.util.function.Function;

/**
 * @author chen.zhu
 * <p>
 * Apr 23, 2018
 */
public class EventModel<T> implements java.io.Serializable {

    private EventType eventType = EventType.UNKNOWN;

    private String eventOperator = "";

    private String eventOperation = "";

    private T eventDetail;

    private String eventProperty = "";

    private Date lastUpdate;


    public EventModel fromEventTbl(EventTbl eventTbl, Function<String, T> transferFunction) {
        setEventType(EventType.valueOf(eventTbl.getEventType()));
        setEventOperator(eventTbl.getEventOperator());
        setEventOperation(eventTbl.getEventOperation());
        setEventDetail(transferFunction.apply(eventTbl.getEventDetail()));
        setLastUpdate(eventTbl.getDataChangeLastTime());
        setEventProperty(eventTbl.getEventProperty());
        return this;
    }

    public EventTbl toEventTbl(Function<T, String> transferFunction) {
        EventTbl eventTbl = new EventTbl();
        eventTbl.setEventType(eventType.name()).setEventOperator(eventOperator).setEventOperation(eventOperation)
                .setEventProperty(eventProperty);
        if(eventDetail != null) {
            try {
                eventTbl.setEventDetail(transferFunction.apply(eventDetail));
            } catch (Exception ignore) {
                eventTbl.setEventDetail("");
            }
        }
        if(lastUpdate != null) {
            eventTbl.setDataChangeLastTime(lastUpdate);
        }
        return eventTbl;
    }

    public EventModel() {
    }

    public EventModel(EventType eventType, String eventOperator, String eventOperation) {
        this.eventType = eventType;
        this.eventOperator = eventOperator;
        this.eventOperation = eventOperation;
    }

    public EventModel(EventType eventType, String eventOperator, String eventOperation, T eventDetail) {
        this.eventType = eventType;
        this.eventOperator = eventOperator;
        this.eventOperation = eventOperation;
        this.eventDetail = eventDetail;
    }

    public EventType getEventType() {
        return eventType;
    }

    public EventModel<T> setEventType(EventType eventType) {
        this.eventType = eventType;
        return this;
    }

    public String getEventOperator() {
        return eventOperator;
    }

    public EventModel<T> setEventOperator(String eventOperator) {
        this.eventOperator = eventOperator;
        return this;
    }

    public String getEventOperation() {
        return eventOperation;
    }

    public EventModel<T> setEventOperation(String eventOperation) {
        this.eventOperation = eventOperation;
        return this;
    }

    public T getEventDetail() {
        return eventDetail;
    }

    public EventModel<T> setEventDetail(T eventDetail) {
        this.eventDetail = eventDetail;
        return this;
    }

    public String getEventProperty() {
        return eventProperty;
    }

    public EventModel<T> setEventProperty(String eventProperty) {
        this.eventProperty = eventProperty;
        return this;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public EventModel<T> setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
        return this;
    }

    public static enum EventType {
        ALERT_EMAIL,
        UNKNOWN,
    }

}
