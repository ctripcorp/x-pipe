package com.ctrip.xpipe.redis.console.service.template;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Created by zhuchen on 2017/8/29.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrganizationTemplate {
    private String message;
    private String total;
    private String status;
    private List<OrganizationInfo> data;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<OrganizationInfo> getData() {
        return data;
    }

    public void setData(List<OrganizationInfo> data) {
        this.data = data;
    }

    @Override public String toString() {
        return "OrganizationTemplate{" + "message='" + message + '\'' + ", total='" + total + '\'' + ", status='"
            + status + '\'' + ", data=" + data + '}';
    }
}
