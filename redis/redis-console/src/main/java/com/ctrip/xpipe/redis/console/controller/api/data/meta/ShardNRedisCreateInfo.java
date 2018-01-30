package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import com.google.common.collect.Maps;

import java.util.*;

/**
 * @author chen.zhu
 * <p>
 * Jan 29, 2018
 */
public class ShardNRedisCreateInfo extends ShardCreateInfo {

    private Map<String, List<String>> dc2Redis;

    public ShardNRedisCreateInfo() {
        super();
    }

    @Override
    public void check() throws CheckFailException {
        //ignore
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }

    public Map<String, List<Pair<String, Integer>>> getDcMappedRedisAddr() {
        Map<String, List<Pair<String, Integer>>> result = Maps.newHashMapWithExpectedSize(dc2Redis.size());
        for(Map.Entry<String, List<String>> entry : dc2Redis.entrySet()) {
            result.put(entry.getKey(), transformRedisAddress(entry.getValue()));
        }
        return result;
    }

    private List<Pair<String,Integer>> transformRedisAddress(List<String> redises) {

        Set<Pair<String,Integer>> set = new HashSet<>();
        redises.forEach(addr -> set.add(IpUtils.parseSingleAsPair(addr)));
        return new LinkedList<>(set);
    }

    public Map<String, List<String>> getDc2Redis() {
        return dc2Redis;
    }

    public ShardNRedisCreateInfo setDc2Redis(Map<String, List<String>> dc2Redis) {
        this.dc2Redis = dc2Redis;
        return this;
    }
}
