package com.ctrip.xpipe.redis.core.proxy.monitor;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class SocketStatsResult implements Serializable {

    private List<String> result;

    private long timestamp;

    public SocketStatsResult() {
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SocketStatsResult that = (SocketStatsResult) o;
        if(timestamp != that.timestamp) {
            return false;
        }
        return ObjectUtils.equals(result, that.result, new ObjectUtils.EqualFunction<List<String>>() {
            @Override
            public boolean equals(List<String> obj1, List<String> obj2) {
                if(obj1 == null || obj2 == null) {
                    return false;
                }
                if(obj1.size() != obj2.size()) {
                    return false;
                }
                for(int i = 0; i < obj1.size(); i++) {
                    if(!obj1.get(i).equalsIgnoreCase(obj2.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        });
    }

    @Override
    public int hashCode() {

        return Objects.hash(result, timestamp);
    }

    @Override
    public String toString() {
        return "SocketStatsResult{" +
                "result=" + Arrays.deepToString(result.toArray(new String[0])) +
                ", timestamp=" + DateTimeUtils.timeAsString(timestamp) +

                '}';
    }
}
