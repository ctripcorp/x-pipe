package com.ctrip.xpipe.redis.checker.controller.result;

import java.util.Objects;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
public class RetMessage {

    public static final String SUCCESS = "success";

    public static final int WARNING_STATE = 1;
    public static final int SUCCESS_STATE = 0;
    public static final int FAIL_STATE = -1;

    private int state;

    private String message;

    public static RetMessage createFailMessage(String message){
        return new RetMessage(FAIL_STATE, message);
    }

    public static RetMessage createSuccessMessage(){
        return createSuccessMessage(SUCCESS);
    }

    public static RetMessage createSuccessMessage(String message){
        return new RetMessage(SUCCESS_STATE, message);
    }

    public static RetMessage createWarningMessage(String message) {
        return new RetMessage(WARNING_STATE, message);
    }

    public RetMessage(){

    }

    public RetMessage(int state){
        this.state = state;
    }

    public RetMessage(int state, String message){
        this.state = state;
        this.message = message;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RetMessage that = (RetMessage) o;
        return state == that.state &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, message);
    }

}

