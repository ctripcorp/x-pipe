package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import java.util.Map;

public class RedisCheckRule {

    private String checkType;

    private Map<String, String> params;

    public RedisCheckRule() {
    }

    public RedisCheckRule(String checkType, Map<String, String> params) {
        this.checkType = checkType;
        this.params = params;
    }

    public String getCheckType() {
        return checkType;
    }

    public void setCheckType(String checkType) {
        this.checkType = checkType;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return "RedisCheckRule{" +
                "checkType='" + checkType + '\'' +
                ", params=" + params +
                '}';
    }
}
