package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbStreamParseFailException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.StreamID;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.StreamNack;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2022/6/23
 */
public class RdbStreamConsumerGroupParser extends AbstractRdbParser<byte[]> implements RdbParser<byte[]> {

    private RdbParseContext context;

    private RdbParser<byte[]> rdbStringParser;

    private byte[] name;

    private RdbLength ms;

    private RdbLength seq;

    private Map<StreamID, StreamNack> pel;

    private RdbLength pelLen;

    private ByteBuf nackRawId;

    private Long nackDeliveryMs;

    private RdbLength nackDeliverCnt;

    private RdbLength consumerLen;

    private byte[] consumerName;

    private Long consumerSeenTime;

    private RdbLength consumerPelLen;

    private long pelReadCnt;

    private long consumerReadCnt;

    private long consumerPelReadCnt;

    private STATE state = STATE.READ_INIT;

    private static final Logger logger = LoggerFactory.getLogger(RdbStreamConsumerGroupParser.class);

    private static final String XGROUP_OP_CREATE = "CREATE";
    private static final String XGROUP_OP_SETID = "SETID";
    private static final String XGROUP_OP_CREATECONSUMER = "CREATECONSUMER";
    private static final String XGROUP_OP_MKSTREAM = "MKSTREAM";

    private static final String XCLAIM_OP_TIME = "TIME";
    private static final String XCLAIM_OP_RETRYCOUNT = "RETRYCOUNT";
    private static final String XCLAIM_OP_FORCE = "FORCE";
    private static final String XCLAIM_OP_JUSTID = "JUSTID";

    enum STATE {
        READ_INIT,
        READ_NAME,
        READ_MS,
        READ_SEQ,
        READ_PEL_LEN,
        READ_NACK_STREAM_ID,
        READ_NACK_DELIVERY_TIME,
        READ_NACK_DELIVERY_COUNT,
        READ_CONSUMER_LEN,
        READ_CONSUMER_NAME,
        READ_CONSUMER_SEEN_TIME,
        READ_CONSUMER_PEL_LEN,
        READ_CONSUMER_NACK_ID,
        READ_END
    }

    public RdbStreamConsumerGroupParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
        parseContext.bindRdbParser(this);
    }

    @Override
    public byte[] read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {

                case READ_INIT:
                    reset();
                    state = STATE.READ_NAME;
                    break;

                case READ_NAME:
                    name = rdbStringParser.read(byteBuf);
                    if (null != name) {
                        rdbStringParser.reset();
                        state = STATE.READ_MS;
                    }
                    break;

                case READ_MS:
                    ms = parseRdbLength(byteBuf);
                    if (null != ms) state = STATE.READ_SEQ;
                    break;

                case READ_SEQ:
                    seq = parseRdbLength(byteBuf);
                    if (null != seq) {
                        propagateCGroupCreate(name, new StreamID(ms.getLenLongValue(), seq.getLenValue()));
                        ms = seq = null;
                        state = STATE.READ_PEL_LEN;
                    }
                    break;

                case READ_PEL_LEN:
                    pelLen = parseRdbLength(byteBuf);
                    if (null != pelLen) {
                        if (pelLen.getLenLongValue() > 0) {
                            state = STATE.READ_NACK_STREAM_ID;
                        } else {
                            state = STATE.READ_CONSUMER_LEN;
                        }
                    }
                    break;

                case READ_NACK_STREAM_ID:
                    nackRawId = readUntilBytesEnough(byteBuf, nackRawId, StreamID.SIZE_STREAM_ID);
                    if (nackRawId.readableBytes() == StreamID.SIZE_STREAM_ID) state = STATE.READ_NACK_DELIVERY_TIME;
                    break;

                case READ_NACK_DELIVERY_TIME:
                    nackDeliveryMs = readMillSecond(byteBuf, context);
                    if (null != nackDeliveryMs) state = STATE.READ_NACK_DELIVERY_COUNT;
                    break;

                case READ_NACK_DELIVERY_COUNT:
                    nackDeliverCnt = parseRdbLength(byteBuf);
                    if (null != nackDeliverCnt) {
                        StreamID nackId = new StreamID(nackRawId);
                        pel.put(nackId, new StreamNack(nackId, nackDeliveryMs, nackDeliverCnt.getLenLongValue()));
                        nackRawId.release();
                        nackRawId = null;
                        nackDeliveryMs = null;
                        nackDeliverCnt = null;

                        pelReadCnt++;
                        if (pelReadCnt >= pelLen.getLenLongValue()) {
                            state = STATE.READ_CONSUMER_LEN;
                        } else {
                            state = STATE.READ_NACK_STREAM_ID;
                        }
                    }
                    break;

                case READ_CONSUMER_LEN:
                    consumerLen = parseRdbLength(byteBuf);
                    if (null != consumerLen) {
                        if (consumerLen.getLenLongValue() > 0) {
                            state = STATE.READ_CONSUMER_NAME;
                        } else {
                            state = STATE.READ_END;
                        }
                    }
                    break;

                case READ_CONSUMER_NAME:
                    consumerName = rdbStringParser.read(byteBuf);
                    if (null != consumerName) {
                        rdbStringParser.reset();
                        propagateConsumerCreate(name, consumerName);
                        state = STATE.READ_CONSUMER_SEEN_TIME;
                    }
                    break;

                case READ_CONSUMER_SEEN_TIME:
                    consumerSeenTime = readMillSecond(byteBuf, context);
                    if (null != consumerSeenTime) state = STATE.READ_CONSUMER_PEL_LEN;
                    break;

                case READ_CONSUMER_PEL_LEN:
                    consumerPelLen = parseRdbLength(byteBuf);
                    if (null != consumerPelLen) {
                        if (consumerPelLen.getLenLongValue() > 0) {
                            state = STATE.READ_CONSUMER_NACK_ID;
                        } else {
                            consumerReadEnd();
                        }
                    }
                    break;

                case READ_CONSUMER_NACK_ID:
                    nackRawId = readUntilBytesEnough(byteBuf, nackRawId, StreamID.SIZE_STREAM_ID);
                    if (nackRawId.readableBytes() == StreamID.SIZE_STREAM_ID) {
                        StreamID consumerNackId = new StreamID(nackRawId);
                        if (!pel.containsKey(consumerNackId)) {
                            throw new RdbStreamParseFailException(context.getKey(), "consumer nackId miss " + consumerNackId);
                        }
                        propagateConsumerNack(name, consumerName, pel.get(consumerNackId));

                        nackRawId.release();
                        nackRawId = null;
                        consumerPelReadCnt++;
                        if (consumerPelReadCnt >= consumerPelLen.getLenLongValue()) {
                            consumerReadEnd();
                        } else {
                            state = STATE.READ_CONSUMER_NACK_ID;
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

        if (isFinish()) return name;
        else return null;
    }

    private void consumerReadEnd() {
        consumerName = null;
        consumerSeenTime = null;
        consumerPelLen = null;
        consumerPelReadCnt = 0;

        consumerReadCnt++;
        if (consumerReadCnt >= consumerLen.getLenLongValue()) {
            state = STATE.READ_END;
        } else {
            state = STATE.READ_CONSUMER_NAME;
        }
    }

    // XGROUP CREATE <key> <groupname> <id> MKSTREAM
    // XGROUP SETID <key> <groupname> <id>
    private void propagateCGroupCreate(byte[] groupName, StreamID streamID) {
        if (null == groupName || null == streamID || null == context.getKey()) {
            return;
        }

        RedisKey key = context.getKey();
        // if consumer group already exist, id will be ignored
        notifyRedisOp(new RedisOpSingleKey(RedisOpType.XGROUP,
                new byte[][] {
                        RedisOpType.XGROUP.name().getBytes(), XGROUP_OP_CREATE.getBytes(),
                        key.get(), groupName, streamID.toString().getBytes(), XGROUP_OP_MKSTREAM.getBytes()},
                key, null));
        notifyRedisOp(new RedisOpSingleKey(RedisOpType.XGROUP,
                new byte[][] {
                        RedisOpType.XGROUP.name().getBytes(), XGROUP_OP_SETID.getBytes(),
                        key.get(), groupName, streamID.toString().getBytes()
                }, key, null));
    }

    // XGROUP CREATECONSUMER <key> <groupname> <consumerName>
    private void propagateConsumerCreate(byte[] groupName, byte[] consumerName) {
        if (null == groupName || null == consumerName || null == context.getKey()) {
            return;
        }

        RedisKey key = context.getKey();
        notifyRedisOp(new RedisOpSingleKey(RedisOpType.XGROUP,
                new byte[][] {
                        RedisOpType.XGROUP.name().getBytes(), XGROUP_OP_CREATECONSUMER.getBytes(),
                        key.get(), groupName, consumerName},
                key, null));
    }

    // XCLAIM <key> <group> <consumer> 0 <id> TIME <milliseconds> RETRYCOUNT <count> FORCE JUSTID
    private void propagateConsumerNack(byte[] groupName, byte[] consumerName, StreamNack nack) {
        if (null == groupName || null == consumerName || null == nack || null == context.getKey()) {
            return;
        }

        RedisKey key = context.getKey();
        notifyRedisOp(new RedisOpSingleKey(RedisOpType.XCLAIM,
                new byte[][] {
                        RedisOpType.XCLAIM.name().getBytes(), key.get(),
                        groupName, consumerName, "0".getBytes(), nack.getStreamIdBytes(),
                        XCLAIM_OP_TIME.getBytes(), nack.getDeliveryMillSecondBytes(),
                        XCLAIM_OP_RETRYCOUNT.getBytes(), nack.getDeliveryCntBytes(),
                        XCLAIM_OP_FORCE.getBytes(), XCLAIM_OP_JUSTID.getBytes()},
                key, null));
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
        if (nackRawId != null) {
            nackRawId.release();
        }
        name = null;
        ms = null;
        seq = null;
        pelLen = null;
        pelReadCnt = 0;
        nackRawId = null;
        nackDeliveryMs = null;
        nackDeliverCnt = null;
        pel = new LinkedHashMap<>();
        consumerLen = null;
        consumerName = null;
        consumerSeenTime = null;
        consumerPelLen = null;
        consumerReadCnt = 0;
        consumerPelReadCnt = 0;

        state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
