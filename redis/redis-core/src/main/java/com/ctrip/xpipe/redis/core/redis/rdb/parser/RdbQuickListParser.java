package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseEmptyKeyException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Ziplist;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/6/17
 */
public class RdbQuickListParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private RdbLength len;

    private int readCnt;

    private byte[] temp;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbQuickListParser.class);

    enum STATE {
        READ_INIT,
        READ_LEN,
        READ_ZL_AS_STR,
        DECODE_ZL,
        READ_END
    }

    public RdbQuickListParser(RdbParseContext parseContext) {
        this.context = parseContext;
        rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Integer read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    len = null;
                    readCnt = 0;
                    state = STATE.READ_LEN;

                case READ_LEN:
                    len = parseRdbLength(byteBuf);
                    if (null != len) {
                        if (len.getLenValue() > 0) {
                            state = STATE.READ_ZL_AS_STR;
                        } else {
                            throw new RdbParseEmptyKeyException("quick list key:" + context.getKey());
                        }
                    }
                    break;

                case READ_ZL_AS_STR:
                    temp = rdbStringParser.read(byteBuf);
                    if (null == temp) break;

                    rdbStringParser.reset();
                    state = STATE.DECODE_ZL;

                case DECODE_ZL:
                    Ziplist ziplist = new Ziplist(temp);
                    temp = null;
                    readCnt++;

                    if (readCnt >= len.getLenValue()) {
                        state = STATE.READ_END;
                    } else {
                        state = STATE.READ_ZL_AS_STR;
                    }

                    propagateCmdIfNeed(ziplist);
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

    private void propagateCmdIfNeed(Ziplist ziplist) {
        if (null == context.getKey() || null == ziplist) return;

        List<byte[]> arr = ziplist.convertToList();
        RedisKey key = context.getKey();
        int i = 0;
        for (byte[] val: arr) {
            if(i == arr.size() -1 && isFinish()){
                notifyRedisOp(new RedisOpSingleKey(
                        RedisOpType.RPUSH,
                        new byte[][] {RedisOpType.RPUSH.name().getBytes(), key.get(), val},
                        key, val, true));
            }else {
                notifyRedisOp(new RedisOpSingleKey(
                        RedisOpType.RPUSH,
                        new byte[][] {RedisOpType.RPUSH.name().getBytes(), key.get(), val},
                        key, val));
            }

            i++;
        }
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        if (rdbStringParser != null) {
            rdbStringParser.reset();
        }
        this.state = STATE.READ_INIT;
        temp = null;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
