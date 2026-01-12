package com.ctrip.xpipe.redis.keeper.container.resp;

public class RestResp<T> {

    private int code;

    private String message;

    private T data;

    public RestResp(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static <T> RestResp<T> success(T data) {
        return new RestResp<>(0, "SUCCESS", data);
    }

    public static <T> RestResp<T> fail(int code, String message) {
        return new RestResp<>(code, message, null);
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public T getData() {
        return data;
    }
}
