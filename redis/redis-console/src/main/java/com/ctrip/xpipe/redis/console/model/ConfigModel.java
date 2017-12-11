package com.ctrip.xpipe.redis.console.model;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
public class ConfigModel {

    private String key;
    private String val;
    private String updateUser;
    private String updateIP;

    public String getUpdateUser() {
        return updateUser;
    }

    public ConfigModel setUpdateUser(String updateUser) {
        this.updateUser = updateUser;
        return this;
    }

    public String getUpdateIP() {
        return updateIP;
    }

    public ConfigModel setUpdateIP(String updateIP) {
        this.updateIP = updateIP;
        return this;
    }

    public String getKey() {
        return key;
    }

    public ConfigModel setKey(String key) {
        this.key = key;
        return this;
    }

    public String getVal() {
        return val;
    }

    public ConfigModel setVal(String val) {
        this.val = val;
        return this;
    }

    @Override
    public String toString() {
        return "{ key: " + key + ", value: " + val + ", updateUser: " + updateUser + ", updateIP: " + updateIP + "}";
    }
}
