package com.ctrip.xpipe.redis.console.model;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
public class ConfigModel {

    private String key;
    private String val;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    @Override
    public String toString() {
        return "{ key: " + key + ", value: " + val + "}";
    }
}
