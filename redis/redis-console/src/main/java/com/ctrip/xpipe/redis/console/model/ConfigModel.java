package com.ctrip.xpipe.redis.console.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigModel {

    private String key;
    private String subKey;
    private String val;
    private String updateUser;
    private String updateIP;

    public ConfigModel() {
    }

    public ConfigModel(ConfigTbl configTbl) {
        if (null == configTbl) return;

        this.key = configTbl.getKey();
        this.subKey = configTbl.getSubKey();
        this.val = configTbl.getValue();
        this.updateIP = configTbl.getLatestUpdateIp();
        this.updateUser = configTbl.getLatestUpdateUser();
    }

    public String getSubKey() {
        return subKey;
    }

    public ConfigModel setSubKey(String subKey) {
        this.subKey = subKey;
        return this;
    }

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
