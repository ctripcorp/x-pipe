package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2022/6/23
 */
public class StreamEntry {

    private StreamID streamID;

    private boolean deleted;

    private LinkedHashMap<byte[], byte[]> fields;

    public StreamEntry(StreamID streamID, boolean deleted, LinkedHashMap<byte[], byte[]> fields) {
        this.streamID = streamID;
        this.deleted = deleted;
        this.fields = fields;
    }

    public StreamEntry(StreamID streamID, boolean deleted) {
        this(streamID, deleted, new LinkedHashMap<>());
    }

    public void addField(byte[] field, byte[] value) {
        this.fields.put(field, value);
    }

    public StreamID getStreamID() {
        return streamID;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public Map<byte[], byte[]> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    public RedisSingleKeyOp buildRedisOp(RedisKey key) {
        if (deleted) {
            return new RedisOpSingleKey(RedisOpType.XDEL,
                    new byte[][] {RedisOpType.XDEL.name().getBytes(), key.get(), streamID.toString().getBytes()},
                    key, null);
        } else {
            byte[][] rawArgs = new byte[3 + 2 * fields.size()][];
            rawArgs[0] = RedisOpType.XADD.name().getBytes();
            rawArgs[1] = key.get();
            rawArgs[2] = streamID.toString().getBytes();

            int i = 3;
            for (Map.Entry<byte[], byte[]> fieldValuePair: fields.entrySet()) {
                rawArgs[i++] = fieldValuePair.getKey();
                rawArgs[i++] = fieldValuePair.getValue();
            }

            return new RedisOpSingleKey(RedisOpType.XADD, rawArgs, key, null);
        }
    }

}
