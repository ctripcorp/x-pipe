package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseEmptyKeyException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Listpack;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.StreamID;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.StreamListpackIterator;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/18
 */
public class RdbStreamListpacksParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private RdbParser<byte[]> streamConsumerGroupParser;

    private RdbLength listpacksLen;

    private int readListpacks;

    private byte[] streamIdStr;

    private byte[] listpackStr;

    private RdbLength length;

    private RdbLength lastIdMs;

    private RdbLength lastIdSeq;

    private RdbLength cgroupsLen;

    private long cgroupReadCnt;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbStreamListpacksParser.class);

    enum STATE {
        READ_INIT,
        READ_LISTPACKS_LEN,
        READ_LISTPACK_MASTER_ID,
        READ_LISTPACK,
        READ_LENGTH,
        READ_LAST_ID_MS,
        READ_LAST_ID_SEQ,
        READ_CONSUME_GROUP_LEN,
        READ_CONSUMER_GROUP,
        READ_END
    }

    public RdbStreamListpacksParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
        this.streamConsumerGroupParser = new RdbStreamConsumerGroupParser(parseContext);
    }

    @Override
    public Integer read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    listpacksLen = null;
                    length = null;
                    readListpacks = 0;
                    streamIdStr = null;
                    listpackStr = null;
                    state = STATE.READ_LISTPACKS_LEN;
                    break;

                case READ_LISTPACKS_LEN:
                    listpacksLen = parseRdbLength(byteBuf);
                    if (null != listpacksLen) {
                        if (listpacksLen.getLenValue() > 0) state = STATE.READ_LISTPACK_MASTER_ID;
                        else state = STATE.READ_LENGTH;
                    }
                    break;

                case READ_LISTPACK_MASTER_ID:
                    streamIdStr = rdbStringParser.read(byteBuf);
                    if (null != streamIdStr) {
                        rdbStringParser.reset();
                        state = STATE.READ_LISTPACK;
                    }
                    break;

                case READ_LISTPACK:
                    listpackStr = rdbStringParser.read(byteBuf);
                    if (null != listpackStr) {
                        rdbStringParser.reset();
                        Listpack listpack = new Listpack(listpackStr);
                        if (0 == listpack.size()) {
                            throw new RdbParseEmptyKeyException(context.getKey(), "listpack empty");
                        }
                        propagateStreamListpack(streamIdStr, listpack);
                        listpackStr = null;

                        readListpacks++;
                        if (readListpacks >= listpacksLen.getLenValue()) state = STATE.READ_LENGTH;
                        else state = STATE.READ_LISTPACK_MASTER_ID;
                    }
                    break;

                case READ_LENGTH:
                    length = parseRdbLength(byteBuf);
                    if (null != length) state = STATE.READ_LAST_ID_MS;
                    break;

                case READ_LAST_ID_MS:
                    lastIdMs = parseRdbLength(byteBuf);
                    if (null != lastIdMs) state = STATE.READ_LAST_ID_SEQ;
                    break;

                case READ_LAST_ID_SEQ:
                    lastIdSeq = parseRdbLength(byteBuf);
                    if (null != lastIdSeq) {
                        propagateLastId(lastIdMs.getLenLongValue(), lastIdSeq.getLenLongValue());
                        state = STATE.READ_CONSUME_GROUP_LEN;
                    }
                    break;

                case READ_CONSUME_GROUP_LEN:
                    cgroupsLen = parseRdbLength(byteBuf);
                    if (null != cgroupsLen) {
                        if (cgroupsLen.getLenLongValue() > 0) {
                            state = STATE.READ_CONSUMER_GROUP;
                        } else {
                            state = STATE.READ_END;
                        }
                    }
                    break;

                case READ_CONSUMER_GROUP:
                    byte[] cgroupName = streamConsumerGroupParser.read(byteBuf);
                    if (null != cgroupName) {
                        streamConsumerGroupParser.reset();

                        cgroupReadCnt++;
                        if (cgroupReadCnt >= cgroupsLen.getLenLongValue()){
                            state = STATE.READ_END;
                        } else {
                            state = STATE.READ_CONSUMER_GROUP;
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

        if (isFinish()) return listpacksLen.getLenValue();
        else return null;
    }

    private void propagateStreamListpack(byte[] streamIdStr, Listpack listpack) {
        if (null == listpack || null == streamIdStr || null == context.getKey()) {
            return;
        }

        StreamListpackIterator iterator = new StreamListpackIterator(new StreamID(streamIdStr), listpack);
        while (iterator.hasNext()) {
            notifyRedisOp(iterator.next().buildRedisOp(context.getKey()));
        }
    }

    private void propagateLastId(long ms, long seq) {
        if (null == context.getKey()) {
            return;
        }

        StreamID lastId = new StreamID(ms, seq);
        RedisKey key = context.getKey();
        byte[] lastIdBytes = lastId.toString().getBytes();
        notifyRedisOp(new RedisOpSingleKey(RedisOpType.XSETID,
                new byte[][] {RedisOpType.XSETID.name().getBytes(), key.get(), lastIdBytes},
                key, lastIdBytes));
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
        if (streamConsumerGroupParser != null) {
            streamConsumerGroupParser.reset();
        }
        listpacksLen = null;
        length = null;
        readListpacks = 0;
        streamIdStr = null;
        listpackStr = null;
        lastIdMs = null;
        lastIdSeq = null;
        cgroupsLen = null;
        cgroupReadCnt = 0;
        this.state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
