package com.ctrip.xpipe.redis.console.reporter;


import java.util.List;
import java.util.Objects;

public class NocReportResponseModel {

    private int code;

    private boolean success;

    private String message;

    private List<Object> data;

    public NocReportResponseModel() {

    }

    public int getCode() {
        return code;
    }

    public NocReportResponseModel setCode(int code) {
        this.code = code;
        return this;
    }

    public boolean isSuccess() {
        return success;
    }

    public NocReportResponseModel setSuccess(boolean success) {
        this.success = success;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public NocReportResponseModel setMessage(String message) {
        this.message = message;
        return this;
    }

    public List<Object> getData() {
        return data;
    }

    public NocReportResponseModel setData(List<Object> data) {
        this.data = data;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NocReportResponseModel that = (NocReportResponseModel) o;
        return code == that.code && success == that.success && Objects.equals(message, that.message) && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, success, message, data);
    }

    @Override
    public String toString() {
        return "MigrationProcessReportResponseModel{" +
                "code=" + code +
                ", success=" + success +
                ", message='" + message + '\'' +
                ", data=" + data +
                '}';
    }
}
