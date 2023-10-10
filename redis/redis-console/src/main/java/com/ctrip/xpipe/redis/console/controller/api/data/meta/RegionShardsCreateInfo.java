package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import org.springframework.util.CollectionUtils;

import java.util.List;

public class RegionShardsCreateInfo extends AbstractCreateInfo {

    private List<String> shardNames;

    public List<String> getShardNames() {
        return shardNames;
    }

    public void setShardNames(List<String> shardNames) {
        this.shardNames = shardNames;
    }

    @Override
    public void check() throws CheckFailException {
        if (CollectionUtils.isEmpty(shardNames)) {
            throw new CheckFailException("shardNames empty");
        }
    }
}
