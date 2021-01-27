package com.ctrip.xpipe.service.beacon.data;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public class BeaconResp<T> {

    public static final int CODE_SUCCESS = 0;
    public static final String MSG_SUCCESS = "success";

    public static final int CODE_ERROR = -1;
    public static final String MSG_ERROR = "error";

    public static final Integer CODE_NOT_FOUND = -2;
    public static final String MSG_NOT_FOUND = "not_found";

    private int code;

    private String msg;

    private T data;

    public BeaconResp() {
        this.code = CODE_SUCCESS;
        this.msg = MSG_SUCCESS;
    }

    public BeaconResp(int code, String message) {
        this.code = code;
        this.msg = message;
    }

    public BeaconResp(T data) {
        this.code = CODE_SUCCESS;
        this.msg = MSG_SUCCESS;
        this.data = data;
    }

    public BeaconResp(int code, String msg, T data) {
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
        return CODE_SUCCESS == this.code;
    }

    @Override
    public String toString() {
        return "BeaconResp{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                ", data=" + data +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeaconResp<?> that = (BeaconResp<?>) o;
        return code == that.code &&
                Objects.equals(msg, that.msg) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, msg, data);
    }

}
