package com.ctrip.xpipe.redis.core.proxy.monitor;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class SocketStatsResult {

    final private List<String> result;

    final private long timestamp;

    public SocketStatsResult(List<String> result) {
        this.result = result;
        this.timestamp = System.currentTimeMillis();
    }

    public SocketStatsResult(List<String> result, long timestamp) {
        this.result = result;
        this.timestamp = timestamp;
    }

    public List<String> getResult() {
        return result;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Object[] toArray() {
        List<Object> response = Lists.newArrayList();
        response.add(timestamp);
        response.addAll(result);
        return response.toArray();
    }

    public ByteBuf toByteBuf() {
        return new ArrayParser(toArray()).format();
    }

    public static SocketStatsResult parseFromArray(Object[] objects) {
        if(!(objects[0] instanceof Long)) {
            throw new XpipeRuntimeException("first element of SocketStatsResult should be timestamp");
        }
        long timestamp = (long) objects[0];
        List<String> strs = Lists.newArrayListWithCapacity(objects.length - 1);
        for(int i = 1; i < objects.length; i++) {
            strs.add(objects[i].toString());
        }
        return new SocketStatsResult(strs, timestamp);
    }
}
