package com.ctrip.xpipe.redis.console.controller.api;

/**
 * @author chen.zhu
 * <p>
 * Jan 23, 2018
 */
public class GenericRetMessage<T> extends RetMessage {

    private int state;

    private T payload;

    public static <T> GenericRetMessage createGenericRetMessage(T t) {
        return new GenericRetMessage<T>(SUCCESS_STATE, t);
    }

    public GenericRetMessage(int state, T payload) {
        this.state = state;
        this.payload = payload;
    }

    public GenericRetMessage(int state) {
        this.state = state;
    }

    @Override
    public int getState() {
        return state;
    }


    public T getPayload() {
        return payload;
    }

    public GenericRetMessage setMessage(T payload) {
        this.payload = payload;
        return this;
    }
}
