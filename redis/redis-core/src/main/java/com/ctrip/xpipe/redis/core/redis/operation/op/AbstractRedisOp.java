package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
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
    }

    @Override
    public String getOpGtid() {
        return gtid;
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

        for (byte[] arg: args) outByteBuf.addComponent(buildArgRESP(arg));
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
        Object[] args = buildRawOpArgs();
        List<String> rawStrs = Stream.of(args).map(Object::toString).collect(Collectors.toList());
        return String.join(" ", rawStrs);
    }
}
