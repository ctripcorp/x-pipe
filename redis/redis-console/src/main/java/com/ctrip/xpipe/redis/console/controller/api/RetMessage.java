package com.ctrip.xpipe.redis.console.controller.api;

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

    public  RetMessage(int state){
        this.state = state;
    }

    public  RetMessage(int state, String message){

        this.state = state;
        this.message  = message;
    }

    public int getState() {
        return state;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setState(int state) {
        this.state = state;
    }
}

