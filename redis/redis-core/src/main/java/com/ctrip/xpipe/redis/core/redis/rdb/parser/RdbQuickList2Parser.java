package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseEmptyKeyException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Listpack;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Ziplist;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author TB
 * @date 2026/3/16 15:00
 */
public class RdbQuickList2Parser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private enum State { READ_INIT, READ_LEN, READ_CONTAINER, READ_DATA, READ_END }
    private State state = State.READ_INIT;
    private RdbLength len;
    private int readCnt = 0;
    private int containerType; // 0: listpack, 1: plain
    private RdbParser<byte[]> rdbStringParser;
    private static final Logger logger = LoggerFactory.getLogger(RdbQuickList2Parser.class);

    public RdbQuickList2Parser(RdbParseContext context) {
        this.context = context;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Integer read(ByteBuf byteBuf) {
        while (!isFinish() && byteBuf.readableBytes() > 0) {
            switch (state) {
                case READ_INIT:
                    len = null;
                    readCnt = 0;
                    state = State.READ_LEN;

                case READ_LEN:
                    len = parseRdbLength(byteBuf);
                    if (len != null) {
                        if (len.getLenValue() > 0) {
                            state = State.READ_CONTAINER;
                        } else {
                            throw new RdbParseEmptyKeyException("quick list key:" + context.getKey());
                        }
                    }
                    break;
                case READ_CONTAINER:
                    RdbLength containerLen = parseRdbLength(byteBuf);
                    if (containerLen != null) {
                        containerType = containerLen.getLenValue();
                        state = State.READ_DATA;
                    }
                    break;
                case READ_DATA:
                    byte[] data = rdbStringParser.read(byteBuf);
                    rdbStringParser.reset();
                    if (data != null) {
                        readCnt++;

                        if (readCnt >= len.getLenValue()) {
                            state = State.READ_END;
                        } else {
                            state = State.READ_CONTAINER;
                        }

                        List<byte[]> elements;
                        if (containerType == RdbConstant.QUICKLIST_NODE_CONTAINER_PACKED) {
                            elements = new Listpack(data).convertToList();
                        } else {
                            elements = List.of(data);
                        }

                        propagateCmdIfNeed(elements);
                    }
                    break;
                case READ_END:
                default:
                    break;
            }

            if (isFinish()) {
                propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
            }

        }
        if (isFinish()) return len.getLenValue();
        else return null;
    }

    private void propagateCmdIfNeed(List<byte[]> elements) {
        if (null == context.getKey() || null == elements) return;

        RedisKey key = context.getKey();
        int i = 0;
        for (byte[] val: elements) {
            if(i == elements.size() -1 && isFinish()){
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
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public boolean isFinish() {
        return State.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        if (rdbStringParser != null) {
            rdbStringParser.reset();
        }
        this.state = State.READ_INIT;
    }
}
