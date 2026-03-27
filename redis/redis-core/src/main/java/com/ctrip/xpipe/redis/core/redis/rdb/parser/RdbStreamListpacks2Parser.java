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

import java.util.ArrayList;
import java.util.List;

/**
 * @author TB
 * @date 2026/3/20 17:38
 */

public class RdbStreamListpacks2Parser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private RdbParseContext context;
    private RdbParser<byte[]> rdbStringParser;
    private RdbParser<byte[]> streamConsumerGroup2Parser; // 使用v2消费者组解析器

    private RdbLength listpacksLen;
    private int readListpacks;
    private byte[] streamIdStr;
    private byte[] listpackStr;
    private RdbLength length;
    private RdbLength lastIdMs;
    private RdbLength lastIdSeq;
    // v2新增字段
    private RdbLength firstIdMs;
    private RdbLength firstIdSeq;
    private RdbLength maxDeletedIdMs;
    private RdbLength maxDeletedIdSeq;
    private RdbLength entriesAdded;
    private RdbLength cgroupsLen;
    private long cgroupReadCnt;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbStreamListpacks2Parser.class);

    enum STATE {
        READ_INIT,
        READ_LISTPACKS_LEN,
        READ_LISTPACK_MASTER_ID,
        READ_LISTPACK,
        READ_LENGTH,
        READ_LAST_ID_MS,
        READ_LAST_ID_SEQ,
        READ_FIRST_ID_MS,
        READ_FIRST_ID_SEQ,
        READ_MAX_DELETED_ID_MS,
        READ_MAX_DELETED_ID_SEQ,
        READ_ENTRIES_ADDED,
        READ_CONSUME_GROUP_LEN,
        READ_CONSUMER_GROUP,
        READ_END
    }

    public RdbStreamListpacks2Parser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
        this.streamConsumerGroup2Parser = new RdbStreamConsumerGroup2Parser(parseContext);
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
                    if (null != lastIdSeq) state = STATE.READ_FIRST_ID_MS;
                    break;

                case READ_FIRST_ID_MS:
                    firstIdMs = parseRdbLength(byteBuf);
                    if (null != firstIdMs) state = STATE.READ_FIRST_ID_SEQ;
                    break;

                case READ_FIRST_ID_SEQ:
                    firstIdSeq = parseRdbLength(byteBuf);
                    if (null != firstIdSeq) state = STATE.READ_MAX_DELETED_ID_MS;
                    break;

                case READ_MAX_DELETED_ID_MS:
                    maxDeletedIdMs = parseRdbLength(byteBuf);
                    if (null != maxDeletedIdMs) state = STATE.READ_MAX_DELETED_ID_SEQ;
                    break;

                case READ_MAX_DELETED_ID_SEQ:
                    maxDeletedIdSeq = parseRdbLength(byteBuf);
                    if (null != maxDeletedIdSeq) state = STATE.READ_ENTRIES_ADDED;
                    break;

                case READ_ENTRIES_ADDED:
                    entriesAdded = parseRdbLength(byteBuf);
                    if (null != entriesAdded) {
                        propagateLastId(lastIdMs.getLenLongValue(), lastIdSeq.getLenLongValue(),
                                entriesAdded.getLenLongValue(),
                                maxDeletedIdMs.getLenLongValue(), maxDeletedIdSeq.getLenLongValue());
                        state = STATE.READ_CONSUME_GROUP_LEN;
                    }
                    break;

                case READ_CONSUME_GROUP_LEN:
                    cgroupsLen = parseRdbLength(byteBuf);
                    if (null != cgroupsLen) {
                        if (cgroupsLen.getLenLongValue() > 0) state = STATE.READ_CONSUMER_GROUP;
                        else state = STATE.READ_END;
                    }
                    break;

                case READ_CONSUMER_GROUP:
                    byte[] cgroupName = streamConsumerGroup2Parser.read(byteBuf);
                    if (null != cgroupName) {
                        streamConsumerGroup2Parser.reset();
                        cgroupReadCnt++;
                        if (cgroupReadCnt >= cgroupsLen.getLenLongValue()) state = STATE.READ_END;
                        else state = STATE.READ_CONSUMER_GROUP;
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

        return isFinish() ? listpacksLen.getLenValue() : null;
    }

    private void propagateStreamListpack(byte[] streamIdStr, Listpack listpack) {
        if (null == listpack || null == streamIdStr || null == context.getKey()) return;
        StreamListpackIterator iterator = new StreamListpackIterator(new StreamID(streamIdStr), listpack);
        while (iterator.hasNext()) {
            notifyRedisOp(iterator.next().buildRedisOp(context.getKey()));
        }
    }

    private void propagateLastId(long ms, long seq, long entriesAdded, long maxDelMs, long maxDelSeq) {
        if (null == context.getKey()) return;
        RedisKey key = context.getKey();
        StreamID lastId = new StreamID(ms, seq);
        byte[] lastIdBytes = lastId.toString().getBytes();
        List<byte[]> args = new ArrayList<>();
        args.add(RedisOpType.XSETID.name().getBytes());
        args.add(key.get());
        args.add(lastIdBytes);
        args.add("ENTRIESADDED".getBytes());
        args.add(Long.toString(entriesAdded).getBytes());
        args.add("MAXDELETEDID".getBytes());
        args.add(new StreamID(maxDelMs, maxDelSeq).toString().getBytes());
        notifyRedisOp(new RedisOpSingleKey(RedisOpType.XSETID, args.toArray(new byte[0][]), key, lastIdBytes));
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        if (rdbStringParser != null) rdbStringParser.reset();
        if (streamConsumerGroup2Parser != null) streamConsumerGroup2Parser.reset();
        listpacksLen = null;
        length = null;
        readListpacks = 0;
        streamIdStr = null;
        listpackStr = null;
        lastIdMs = null;
        lastIdSeq = null;
        firstIdMs = null;
        firstIdSeq = null;
        maxDeletedIdMs = null;
        maxDeletedIdSeq = null;
        entriesAdded = null;
        cgroupsLen = null;
        cgroupReadCnt = 0;
        state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}