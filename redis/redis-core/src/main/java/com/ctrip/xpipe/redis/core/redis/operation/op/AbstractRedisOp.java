package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

import java.util.List;

import static com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol.ASTERISK_BYTE;
import static com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol.DOLLAR_BYTE;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public abstract class AbstractRedisOp implements RedisOp {

    private String gtid;

    private Long timestamp;

    private String gid;

    private List<String> rawArgs;

    public AbstractRedisOp() {}

    public AbstractRedisOp(List<String> rawArgs) {
        this(rawArgs, null, null, null);
    }

    public AbstractRedisOp(List<String> rawArgs, String gtid) {
        this(rawArgs, gtid, null, null);
    }

    public AbstractRedisOp(List<String> rawArgs, String gid, Long timestamp) {
        this(rawArgs, null, gid, timestamp);
    }

    public AbstractRedisOp(List<String> rawArgs, String gtid, String gid, Long timestamp) {
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
    public List<String> buildRawOpArgs() {
        return rawArgs;
    }

    @Override
    public byte[] buildRESP() {
        List<String> args = buildRawOpArgs();
        StringBuilder sb = new StringBuilder(String.format("%c%d%s", ASTERISK_BYTE, args.size(), RedisClientProtocol.CRLF));
        for (String arg: args) {
            sb.append(String.format("%c%d%s", DOLLAR_BYTE, arg.length(), RedisClientProtocol.CRLF));
            sb.append(arg + RedisClientProtocol.CRLF);
        }

        return sb.toString().getBytes();
    }

    protected void setRawArgs(List<String> args) {
        this.rawArgs = args;
    }

    @Override
    public String toString() {
        List<String> args = buildRawOpArgs();
        return String.join(" ", args);
    }
}
