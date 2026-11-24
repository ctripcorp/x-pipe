package com.ctrip.xpipe.redis.console.controller.api.vo;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class BeaconFailoverResponse<T> {

    public static final int CODE_SUCCESS = 0;
    public static final int CODE_ERROR = -1;

    private int code;

    private String msg;

    private T data;

    public BeaconFailoverResponse() {
        this(CODE_SUCCESS, "success", null);
    }

    public BeaconFailoverResponse(T data) {
        this(CODE_SUCCESS, "success", data);
    }

    public BeaconFailoverResponse(int code, String msg) {
        this(code, msg, null);
    }

    public BeaconFailoverResponse(int code, String msg, T data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @JsonIgnore
    public boolean isSuccess() {
        return code == CODE_SUCCESS;
    }
}
