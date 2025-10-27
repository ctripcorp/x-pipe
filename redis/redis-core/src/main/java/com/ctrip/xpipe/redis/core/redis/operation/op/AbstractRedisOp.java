package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol.ASTERISK_BYTE;
import static com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol.DOLLAR_BYTE;
import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.CRLF;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public abstract class AbstractRedisOp implements RedisOp {

    private String gtid;

    private Long timestamp;

    private String gid;

    private byte[][] rawArgs;

    private long estimatedSize;

    public AbstractRedisOp() {}

    public AbstractRedisOp(byte[][] rawArgs) {
        this(rawArgs, null, null, null);
    }

    public AbstractRedisOp(byte[][] rawArgs, String gtid) {
        this(rawArgs, gtid, null, null);
    }

    public AbstractRedisOp(byte[][] rawArgs, String gid, Long timestamp) {
        this(rawArgs, null, gid, timestamp);
    }

    public AbstractRedisOp(byte[][] rawArgs, String gtid, String gid, Long timestamp) {
        this.rawArgs = rawArgs;
        this.gtid = gtid;
        this.gid = gid;
        this.timestamp = timestamp;

        this.estimateSize(rawArgs, 0);
    }

    protected void estimateSize(byte[][] rawArgs, long alongWith) {

        estimatedSize = alongWith;

        if (rawArgs == null) {
            return;
        }

        for (byte[] rawArg : rawArgs) {
            estimatedSize += rawArg.length;
        }
    }

    @Override
    public long estimatedSize() {
        return estimatedSize;
    }

    @Override
    public String getDbId(){
        return "";
    }

    @Override
    public String getOpGtid() {
        return gtid;
    }

    @Override
    public void clearGtid() {
        this.gtid = null;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String getGid() {
        return gid;
    }

    @Override
    public byte[][] buildRawOpArgs() {
        return rawArgs;
    }

    @Override
    public ByteBuf buildRESP() {
        byte[][] args = buildRawOpArgs();
        CompositeByteBuf outByteBuf = Unpooled.compositeBuffer(args.length + 1);
        String arrayLength = String.format("%c%d%s", ASTERISK_BYTE, args.length, CRLF);
        outByteBuf.addComponent(true, Unpooled.wrappedBuffer(arrayLength.getBytes()));

        for (byte[] arg: args) outByteBuf.addComponent(true, buildArgRESP(arg));
        return outByteBuf;
    }

    protected ByteBuf buildArgRESP(byte[] arg) {
        String argLength = String.format("%c%d%s", DOLLAR_BYTE, arg.length, CRLF);
        return Unpooled.wrappedBuffer(argLength.getBytes(), arg, CRLF.getBytes());
    }

    protected void setRawArgs(byte[][] args) {
        this.rawArgs = args;
    }

    @Override
    public String toString() {
        byte[][] args = buildRawOpArgs();
        List<String> rawStrs = Stream.of(args).map(bytes -> new String(bytes, Codec.defaultCharset)).collect(Collectors.toList());
        return String.join(" ", rawStrs);
    }
}
