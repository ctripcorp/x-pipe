package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.utils.StringUtil;

public class RedisCheckRuleCreateInfo extends AbstractCreateInfo{

    private Long id;

    private String checkType;

    private String param;

    private String description;

    @Override
    public void check() throws CheckFailException {

        if(StringUtil.isEmpty(checkType)) {
            throw new CheckFailException("checkType Empty");
        }

        if(StringUtil.isEmpty(param)) {
            throw new CheckFailException("param Empty");
        }

        if(!"info".equals(checkType) && !"config".equals(checkType)){
            throw new CheckFailException("checkType must be info or config");
        }
    }

    @Override
    public String toString() {
        return "RedisCheckRuleCreateInfo{" +
                "id=" + id +
                ", checkType='" + checkType + '\'' +
                ", param='" + param + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    public String getDescription() {
        return description;
    }

    public RedisCheckRuleCreateInfo setDescription(String description) {
        this.description = description;
        return this;
    }

    public Long getId() {
        return id;
    }

    public RedisCheckRuleCreateInfo setId(Long id) {
        this.id = id;
        return this;
    }

    public String getCheckType() {
        return checkType;
    }

    public RedisCheckRuleCreateInfo setCheckType(String checkType) {
        this.checkType = checkType;
        return this;
    }

    public String getParam() {
        return param;
    }

    public RedisCheckRuleCreateInfo setParam(String param) {
        this.param = param;
        return this;
    }

}
