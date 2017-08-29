package com.ctrip.xpipe.redis.console.service.template;

/**
 * Created by zhuchen on 2017/8/29.
 */
public class CMSRequestBody {
    private String access_token;
    private Object request_body;

    public CMSRequestBody(String access_token) {
        this.access_token = access_token;
    }

    public String getAccess_token() {
        return access_token;
    }

    public void setAccess_token(String access_token) {
        this.access_token = access_token;
    }

    public Object getRequest_body() {
        return request_body;
    }

    public void setRequest_body(Object request_body) {
        this.request_body = request_body;
    }
}
