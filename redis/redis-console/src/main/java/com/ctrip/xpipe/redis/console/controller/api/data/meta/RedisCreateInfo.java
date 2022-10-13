package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Jan 30, 2018
 */
public class RedisCreateInfo extends AbstractCreateInfo {
    private String dcId;
    private String shardName;
    private String redises;

    @Override
    public void check() throws CheckFailException {

    }

    public RedisCreateInfo() {
    }

    public String getDcId() {
        return dcId;
    }

    public RedisCreateInfo setDcId(String dcId) {
        this.dcId = dcId;
        return this;
    }

    public String getShardName() {
        return shardName;
    }

    public RedisCreateInfo setShardName(String shardName) {
        this.shardName = shardName;
        return this;
    }

    public String getRedises() {
        return redises;
    }

    public RedisCreateInfo setRedises(String redis) {
        this.redises = redis;
        return this;
    }

    public List<Pair<String,Integer>> getRedisAddresses() {
        if(redises == null || StringUtil.isEmpty(redises))
            throw new IllegalArgumentException("No redises posted");
        String[] redisArray = StringUtil.splitRemoveEmpty("\\s*,\\s*", redises);
        Set<Pair<String, Integer>> addresses = Sets.newHashSetWithExpectedSize(redisArray.length);
        for(String redis : redisArray) {
            addresses.add(IpUtils.parseSingleAsPair(redis));
        }
        return Lists.newArrayList(addresses);
    }
}
