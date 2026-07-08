package com.ctrip.xpipe.redis.console.controller.api.vo;

public class RestResponse<T> {

    public static final int CODE_SUCCESS = 0;
    public static final int CODE_FAIL = -1;

    private int code;
    private String msg;
    private T data;

    public RestResponse() {
    }

    public RestResponse(int code, String msg, T data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public static <T> RestResponse<T> success(T data) {
        return new RestResponse<>(CODE_SUCCESS, "success", data);
    }

    public static <T> RestResponse<T> fail(String msg) {
        return new RestResponse<>(CODE_FAIL, msg, null);
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
}
