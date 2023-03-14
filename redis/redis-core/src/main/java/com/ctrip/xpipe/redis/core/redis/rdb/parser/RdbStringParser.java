package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLenType;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.util.RedisLzfUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant.*;

/**
 * @author lishanglin
 * date 2022/5/29
 */
public class RdbStringParser extends AbstractRdbParser<byte[]> implements RdbParser<byte[]> {

    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private INT_AS_STR_ENC_TYPE intAsStrEncType;

    private RdbLength length;

    private ByteBuf temp;

    private RdbLength clen;

    private RdbLength len;

    private byte[] lzfContent;

    private byte[] redisString;

    private static final Logger logger = LoggerFactory.getLogger(RdbStringParser.class);

    enum STATE {
        READ_INIT,
        READ_LENGTH,
        READ_PLAIN_STRING,
        READ_INT_AS_STRING,
        READ_LZF_CLEN,
        READ_LZF_LEN,
        READ_LZF_CONTENT,
        READ_LZF_DECOMPRESS,
        READ_END
    }

    enum INT_AS_STR_ENC_TYPE {
        ENC_INT8(REDIS_RDB_ENC_INT8, 1, byteBuf -> (long) byteBuf.readByte()),
        ENC_INT16(REDIS_RDB_ENC_INT16, 2, byteBuf -> (long) byteBuf.readShortLE()),
        ENC_INT32(REDIS_RDB_ENC_INT32, 4, byteBuf -> (long) byteBuf.readIntLE());

        private short code;

        private int readBytes;

        private Function<ByteBuf, Long> parser;

        INT_AS_STR_ENC_TYPE(short code, int readBytes, Function<ByteBuf, Long> parser) {
            this.code = code;
            this.readBytes = readBytes;
            this.parser = parser;
        }

        public int getReadBytes() {
            return readBytes;
        }

        public long parse(ByteBuf byteBuf) {
            return this.parser.apply(byteBuf);
        }

        public static INT_AS_STR_ENC_TYPE parse(int type) {
            switch (type) {
                case REDIS_RDB_ENC_INT8:
                    return ENC_INT8;
                case REDIS_RDB_ENC_INT16:
                    return ENC_INT16;
                case REDIS_RDB_ENC_INT32:
                    return ENC_INT32;
                default:
                    return null;
            }
        }

    }

    public RdbStringParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    @Override
    public byte[] read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    length = null;
                    temp = null;
                    redisString = null;
                    state = STATE.READ_LENGTH;
                    intAsStrEncType = null;
                    clen = null;
                    len = null;
                    break;

                case READ_LENGTH:
                    length = parseRdbLength(byteBuf);
                    if (null != length) {
                        if (RdbLenType.LEN_ENC.equals(length.getLenType())) {
                            if (length.getLenValue() == REDIS_RDB_ENC_LZF) {
                                state = STATE.READ_LZF_CLEN;
                            } else {
                                state = STATE.READ_INT_AS_STRING;
                            }
                        } else {
                            state = STATE.READ_PLAIN_STRING;
                        }
                    }
                    break;

                case READ_PLAIN_STRING:
                    temp = readUntilBytesEnough(byteBuf, temp, length.getLenValue());
                    if (temp.readableBytes() == length.getLenValue()) {
                        redisString = new byte[length.getLenValue()];
                        temp.readBytes(redisString);
                        temp.release();
                        temp = null;
                        state = STATE.READ_END;
                    }
                    break;

                case READ_INT_AS_STRING:
                    if (null == intAsStrEncType) intAsStrEncType = INT_AS_STR_ENC_TYPE.parse(length.getLenValue());
                    if (null == intAsStrEncType) throw new XpipeRuntimeException("unknokwn enc type " + length);
                    temp = readUntilBytesEnough(byteBuf, temp, intAsStrEncType.getReadBytes());
                    if (temp.readableBytes() == intAsStrEncType.getReadBytes()) {
                        long val = intAsStrEncType.parse(temp);
                        redisString = String.valueOf(val).getBytes();
                        temp.release();
                        temp = null;
                        state = STATE.READ_END;
                    }
                    break;

                case READ_LZF_CLEN:
                    clen = parseRdbLength(byteBuf);
                    if (null != clen) {
                        state = STATE.READ_LZF_LEN;
                    }
                    break;

                case READ_LZF_LEN:
                    len = parseRdbLength(byteBuf);
                    if (null != len) {
                        state = STATE.READ_LZF_CONTENT;
                    }
                    break;

                case READ_LZF_CONTENT:
                    if (RdbLenType.LEN_ENC.equals(clen.getLenType()) || RdbLenType.LEN_ENC.equals(len.getLenType())) {
                        throw new XpipeRuntimeException("unsupport enc len in lzf enc");
                    }
                    temp = readUntilBytesEnough(byteBuf, temp, clen.getLenValue());
                    if (temp.readableBytes() != clen.getLenValue()) break;

                    lzfContent = new byte[clen.getLenValue()];
                    temp.readBytes(lzfContent);
                    temp.release();
                    temp = null;
                    state = STATE.READ_LZF_DECOMPRESS;

                case READ_LZF_DECOMPRESS:
                    redisString = new byte[len.getLenValue()];
                    if (RedisLzfUtil.decode(lzfContent, redisString) != len.getLenValue()) {
                        throw new XpipeRuntimeException("invalid LZF compressed string");
                    }
                    state = STATE.READ_END;
                    break;

                case READ_END:
                default:
            }

            if (isFinish()) {
                propagateCmdIfNeed();
            }
        }

        return redisString;
    }

    private void propagateCmdIfNeed() {
        if (null == redisString
                || null == context.getKey()
                || !RdbParseContext.RdbType.STRING.equals(context.getCurrentType())) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.SET,
                new byte[][] {RedisOpType.SET.name().getBytes(), context.getKey().get(), redisString},
                context.getKey(), redisString));
        propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        if (temp != null) {
            temp.release();
        }
        this.state = STATE.READ_INIT;
        lzfContent = null;
        redisString = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
