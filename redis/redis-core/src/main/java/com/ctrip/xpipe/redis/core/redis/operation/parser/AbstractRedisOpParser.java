package com.ctrip.xpipe.redis.core.redis.operation.parser;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;

/**
 * @author lishanglin
 * date 2022/5/31
 */
public abstract class AbstractRedisOpParser implements RedisOpParser {

    @Override
    public RedisOp parse(Object[] args) {
        return parse(decodeArgsAsBytesArray(args));
    }

    protected byte[][] decodeArgsAsBytesArray(Object[] args) {
        byte[][] bytesArray = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg instanceof byte[]) bytesArray[i] = (byte[])arg;
            else if (arg instanceof ByteArrayOutputStreamPayload) bytesArray[i] = ((ByteArrayOutputStreamPayload) arg).getBytes();
            else if (arg instanceof String) bytesArray[i] = ((String) arg).getBytes();
            else throw new IllegalArgumentException("arg type" + arg.getClass().getSimpleName() + " couldn't be transformed to byte[]");
        }

        return bytesArray;
    }

    protected String bytes2Str(byte[] arg) {
        return new String(arg, Codec.defaultCharset);
    }

}
