package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseEmptyKeyException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/18
 */
public class RdbZSet2Parser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private STATE state = STATE.READ_INIT;

    private RdbLength len;

    private int readCnt;

    private byte[] member;

    private ByteBuf score;

    private static final Logger logger = LoggerFactory.getLogger(RdbZSet2Parser.class);

    enum STATE {
        READ_INIT,
        READ_LEN,
        READ_MEMBER,
        READ_SCORE,
        READ_END
    }

    public RdbZSet2Parser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Integer read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    len = null;
                    readCnt = 0;
                    member = null;
                    score = null;
                    state = STATE.READ_LEN;
                    break;

                case READ_LEN:
                    len = parseRdbLength(byteBuf);
                    if (null != len) {
                        if (len.getLenValue() > 0) {
                            state = STATE.READ_MEMBER;
                        } else {
                            throw new RdbParseEmptyKeyException("hash key " + context.getKey());
                        }
                    }
                    break;

                case READ_MEMBER:
                    member = rdbStringParser.read(byteBuf);
                    if (null != member) {
                        rdbStringParser.reset();
                        state = STATE.READ_SCORE;
                    }
                    break;

                case READ_SCORE:
                    score = readUntilBytesEnough(byteBuf, score, 8);
                    if (null != score) {
                        rdbStringParser.reset();
                        propagateCmdIfNeed(member, score.readDoubleLE());

                        member = null;
                        score = null;
                        readCnt++;
                        if (readCnt >= len.getLenValue()) {
                            state = STATE.READ_END;
                        } else {
                            state = STATE.READ_MEMBER;
                        }
                    }
                    break;

                case READ_END:
                default:

            }

            if (isFinish()) {
                propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
            }
        }

        if (isFinish()) return len.getLenValue();
        else return null;
    }

    private void propagateCmdIfNeed(byte[] member, double score) {
        if (null == member || null == context.getKey()) {
            return;
        }

        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.ZADD,
                new byte[][] {RedisOpType.ZADD.name().getBytes(), context.getKey().get(), String.valueOf(score).getBytes(), member},
                context.getKey(), member));
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        this.state = STATE.READ_INIT;
        this.member = null;
        this.score = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
