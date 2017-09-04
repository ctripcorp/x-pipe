package com.ctrip.xpipe.service.organization;

import java.util.List;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public class OrgResponseBody {
    private String message;
    private long total;
    private boolean status;
    private List<Data> data;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public List<Data> getData() {
        return data;
    }

    public void setData(List<Data> data) {
        this.data = data;
    }
}
