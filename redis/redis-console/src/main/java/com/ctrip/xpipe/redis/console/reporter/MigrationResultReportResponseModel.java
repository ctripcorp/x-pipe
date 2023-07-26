package com.ctrip.xpipe.redis.console.reporter;

import java.util.List;
import java.util.Objects;

public class MigrationResultReportResponseModel {

    private int code;

    private boolean success;

    private String msg;

    private List<Object> data;

    public MigrationResultReportResponseModel() {
    }

    public int getCode() {
        return code;
    }

    public MigrationResultReportResponseModel setCode(int code) {
        this.code = code;
        return this;
    }

    public boolean isSuccess() {
        return success;
    }

    public MigrationResultReportResponseModel setSuccess(boolean success) {
        this.success = success;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public MigrationResultReportResponseModel setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public List<Object> getData() {
        return data;
    }

    public MigrationResultReportResponseModel setData(List<Object> data) {
        this.data = data;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationResultReportResponseModel that = (MigrationResultReportResponseModel) o;
        return code == that.code && success == that.success && Objects.equals(msg, that.msg) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, success, msg, data);
    }

    @Override
    public String toString() {
        return "MigrationResultReportResponseModel{" +
                "code=" + code +
                ", success=" + success +
                ", msg='" + msg + '\'' +
                ", data=" + data +
                '}';
    }
}
