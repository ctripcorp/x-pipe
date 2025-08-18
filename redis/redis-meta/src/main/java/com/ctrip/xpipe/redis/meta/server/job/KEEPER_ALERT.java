package com.ctrip.xpipe.redis.meta.server.job;

public enum KEEPER_ALERT {
    CHECK_NOT_REDIS("not redis"),
    CHECK_NOT_MASTER("not master"),
    CHECK_MULTI_MASTER("multi master"),
    CHANGE_NOT_FIND("can not find active keeper"),
    COMMAND_FAIL("command fail");

    private String desc;

    KEEPER_ALERT(String desc){
        this.desc = desc;
    }
    public String getDesc() {
        return desc;
    }

}
