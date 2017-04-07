package com.ctrip.xpipe.redis.console.controller.api;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
public class RetMessage {

    public static final String SUCCESS = "success";

    private int state;

    private String message;

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

