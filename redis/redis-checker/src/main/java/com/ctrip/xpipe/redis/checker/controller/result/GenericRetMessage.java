package com.ctrip.xpipe.redis.checker.controller.result;

/**
 * @author chen.zhu
 * <p>
 * Jan 23, 2018
 */
public class GenericRetMessage<T> extends RetMessage {

    private T payload;

    public static <T> GenericRetMessage createGenericRetMessage(T t) {
        return new GenericRetMessage<>(SUCCESS_STATE, t);
    }

    public GenericRetMessage(int state, T payload) {
        super(state);
        this.payload = payload;
    }

    public GenericRetMessage(int state) {
        super(state);
    }

    public GenericRetMessage() {

    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

}
