package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.utils.StringUtil;

import java.util.List;

public class AzGroupCreateInfo extends AbstractCreateInfo {

    private String name;

    private List<String> azs;

    @Override
    public void check() throws CheckFailException {
        if (StringUtil.isEmpty(name)) {
            throw new CheckFailException("name empty");
        }
        if (azs == null || azs.isEmpty()) {
            throw new CheckFailException("azs empty");
        }
        for (String az : azs) {
            if (StringUtil.isEmpty(az)) {
                throw new CheckFailException("az empty");
            }
        }
    }

    public String getName() {
        return name;
    }

    public AzGroupCreateInfo setName(String name) {
        this.name = name;
        return this;
    }

    public List<String> getAzs() {
        return azs;
    }

    public AzGroupCreateInfo setAzs(List<String> azs) {
        this.azs = azs;
        return this;
    }

    @Override
    public String toString() {
        return "AzGroupCreateInfo{" +
                "name='" + name + '\'' +
                ", azs=" + azs +
                '}';
    }
}
