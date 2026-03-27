package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.Listpack;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.StreamID;
import com.ctrip.xpipe.redis.core.redis.rdb.encoding.StreamListpackIterator;
import org.slf4j.LoggerFactory;

/**
 * @author TB
 * @date 2026/3/16 15:01
 */


public class RdbStreamListpacks4Parser extends AbstractRdbParser<Void> {

    private static final Logger logger = LoggerFactory.getLogger(RdbStreamListpacks4Parser.class);

    private final RdbParseContext context;
    private final RdbParser<byte[]> stringParser;
//    private final RdbParser<byte[]> consumerGroupParser; // 可复用 RdbStreamConsumerGroupParser

    // 状态枚举
    private enum State {
        READ_INIT,
        READ_LISTPACKS_LEN,
        READ_LISTPACK_MASTER_ID,
        READ_LISTPACK_DATA,
        READ_STREAM_LENGTH,
        READ_LAST_ID_MS,
        READ_LAST_ID_SEQ,
        READ_FIRST_ID_MS,
        READ_FIRST_ID_SEQ,
        READ_MAX_DELETED_ID_MS,
        READ_MAX_DELETED_ID_SEQ,
        READ_ENTRIES_ADDED,
        READ_CGROUPS_LEN,
        READ_CGROUP,          // 委托给 consumerGroupParser
        READ_IDMP_DURATION,
        READ_IDMP_MAX_ENTRIES,
        READ_IDMP_NUM_PRODUCERS,
        READ_IDMP_PRODUCER_PID,
        READ_IDMP_PRODUCER_COUNT,
        READ_IDMP_ENTRY_IID,
        READ_IDMP_ENTRY_MS,
        READ_IDMP_ENTRY_SEQ,
        READ_IIDS_ADDED,
        READ_IIDS_DUPLICATES,
        FINISH
    }

    private State state = State.READ_INIT;

    // 当前解析的计数器
    private long listpacksLen;               // 节点总数
    private long readListpacks;               // 已读节点数
    private long producersLen;                // IDMP 生产者总数
    private long readProducers;                // 已读生产者数
    private long currentProducerEntries;       // 当前生产者的条目数
    private long readProducerEntries;          // 当前生产者已读条目数
    private long cgroupsLen;                   // 消费者组总数
    private long readCgroups;                   // 已读消费者组数

    // 临时存储
    private byte[] streamIdStr;
    private byte[] listpackBytes;
    private byte[] producerPid;
    private byte[] entryIid;
    private long entryMs;

    // 委托解析器
    private final RdbParser<byte[]> consumerGroupParserInstance;

    public RdbStreamListpacks4Parser(RdbParseContext context) {
        this.context = context;
        this.stringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
        this.consumerGroupParserInstance = new RdbStreamConsumerGroupParser(context); // 假设已有
    }

    @Override
    public Void read(ByteBuf byteBuf) {
        while (!isFinish() && byteBuf.readableBytes() > 0) {
            switch (state) {
                case READ_INIT:
                    resetCounters();
                    state = State.READ_LISTPACKS_LEN;
                    break;

                case READ_LISTPACKS_LEN:
                    RdbLength len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    listpacksLen = len.getLenValue();
                    if (listpacksLen == 0) {
                        // 空流，跳过节点读取
                        state = State.READ_STREAM_LENGTH;
                    } else {
                        state = State.READ_LISTPACK_MASTER_ID;
                    }
                    break;

                case READ_LISTPACK_MASTER_ID:
                    streamIdStr = stringParser.read(byteBuf);
                    if (streamIdStr == null) break;
                    stringParser.reset();
                    state = State.READ_LISTPACK_DATA;
                    break;

                case READ_LISTPACK_DATA:
                    listpackBytes = stringParser.read(byteBuf);
                    if (listpackBytes == null) break;
                    stringParser.reset();
                    // 处理该 listpack 节点
                    processListpackNode(streamIdStr, listpackBytes);
                    readListpacks++;
                    if (readListpacks >= listpacksLen) {
                        state = State.READ_STREAM_LENGTH;
                    } else {
                        state = State.READ_LISTPACK_MASTER_ID;
                    }
                    break;

                case READ_STREAM_LENGTH:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    // 可存储流长度（暂不处理）
                    state = State.READ_LAST_ID_MS;
                    break;

                case READ_LAST_ID_MS:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    long lastMs = len.getLenValue();
                    state = State.READ_LAST_ID_SEQ;
                    break;

                case READ_LAST_ID_SEQ:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    long lastSeq = len.getLenValue();
                    // 暂存最后 ID，可后续生成 XSETID
                    state = State.READ_FIRST_ID_MS;
                    break;

                case READ_FIRST_ID_MS:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    state = State.READ_FIRST_ID_SEQ;
                    break;

                case READ_FIRST_ID_SEQ:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    state = State.READ_MAX_DELETED_ID_MS;
                    break;

                case READ_MAX_DELETED_ID_MS:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    state = State.READ_MAX_DELETED_ID_SEQ;
                    break;

                case READ_MAX_DELETED_ID_SEQ:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    state = State.READ_ENTRIES_ADDED;
                    break;

                case READ_ENTRIES_ADDED:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    state = State.READ_CGROUPS_LEN;
                    break;

                case READ_CGROUPS_LEN:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    cgroupsLen = len.getLenValue();
                    if (cgroupsLen == 0) {
                        state = State.READ_IDMP_DURATION;
                    } else {
                        readCgroups = 0;
                        state = State.READ_CGROUP;
                    }
                    break;

                case READ_CGROUP:
                    // 委托给消费者组解析器，它返回组名（字节数组）表示解析完一个组
                    byte[] groupName = consumerGroupParserInstance.read(byteBuf);
                    if (groupName == null) break;
                    consumerGroupParserInstance.reset();
                    readCgroups++;
                    if (readCgroups >= cgroupsLen) {
                        state = State.READ_IDMP_DURATION;
                    }
                    break;

                case READ_IDMP_DURATION:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    long idmpDuration = len.getLenValue();
                    notifyAux("stream-idmp-duration", String.valueOf(idmpDuration));
                    state = State.READ_IDMP_MAX_ENTRIES;
                    break;

                case READ_IDMP_MAX_ENTRIES:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    long idmpMaxEntries = len.getLenValue();
                    notifyAux("stream-idmp-max-entries", String.valueOf(idmpMaxEntries));
                    state = State.READ_IDMP_NUM_PRODUCERS;
                    break;

                case READ_IDMP_NUM_PRODUCERS:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    producersLen = len.getLenValue();
                    if (producersLen == 0) {
                        state = State.READ_IIDS_ADDED;
                    } else {
                        readProducers = 0;
                        state = State.READ_IDMP_PRODUCER_PID;
                    }
                    break;

                case READ_IDMP_PRODUCER_PID:
                    producerPid = stringParser.read(byteBuf);
                    if (producerPid == null) break;
                    stringParser.reset();
                    state = State.READ_IDMP_PRODUCER_COUNT;
                    break;

                case READ_IDMP_PRODUCER_COUNT:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    currentProducerEntries = len.getLenValue();
                    readProducerEntries = 0;
                    if (currentProducerEntries == 0) {
                        // 无条目的生产者，直接进入下一个生产者或结束
                        afterProducer();
                    } else {
                        state = State.READ_IDMP_ENTRY_IID;
                    }
                    break;

                case READ_IDMP_ENTRY_IID:
                    entryIid = stringParser.read(byteBuf);
                    if (entryIid == null) break;
                    stringParser.reset();
                    state = State.READ_IDMP_ENTRY_MS;
                    break;

                case READ_IDMP_ENTRY_MS:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    entryMs = len.getLenValue();
                    state = State.READ_IDMP_ENTRY_SEQ;
                    break;

                case READ_IDMP_ENTRY_SEQ:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    long entrySeq = len.getLenValue();
                    // 处理一个 IDMP 条目：通过 notifyAux 传递信息
                    notifyAux("stream-idmp-entry",
                            new String(producerPid) + ":" +
                                    new String(entryIid) + ":" +
                                    entryMs + "-" + entrySeq);
                    readProducerEntries++;
                    if (readProducerEntries >= currentProducerEntries) {
                        afterProducer();
                    } else {
                        state = State.READ_IDMP_ENTRY_IID;
                    }
                    break;

                case READ_IIDS_ADDED:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    long iidsAdded = len.getLenValue();
                    notifyAux("stream-iids-added", String.valueOf(iidsAdded));
                    state = State.READ_IIDS_DUPLICATES;
                    break;

                case READ_IIDS_DUPLICATES:
                    len = parseRdbLength(byteBuf);
                    if (len == null) break;
                    long iidsDuplicates = len.getLenValue();
                    notifyAux("stream-iids-duplicates", String.valueOf(iidsDuplicates));
                    state = State.FINISH;
                    break;

                case FINISH:
                    break;
            }
        }

        if (isFinish()) {
            propagateExpireAtIfNeed(context.getKey(), context.getExpireMilli());
        }
        return null;
    }

    private void afterProducer() {
        readProducers++;
        if (readProducers >= producersLen) {
            state = State.READ_IIDS_ADDED;
        } else {
            state = State.READ_IDMP_PRODUCER_PID;
        }
    }

    private void processListpackNode(byte[] streamIdBytes, byte[] listpackBytes) {
        // 将节点数据转换为 XADD 命令，可由 StreamListpackIterator 完成
        // 此处简化，直接使用已有的迭代器
        Listpack listpack = new Listpack(listpackBytes);
        StreamID masterId = new StreamID(streamIdBytes); // 需要实现从字节数组构造
        StreamListpackIterator iterator = new StreamListpackIterator(masterId, listpack);
        while (iterator.hasNext()) {
            // 假设 iterator.next() 返回一个对象可以生成 RedisOp
            // 这里为了简洁，直接调用 notifyRedisOp
            // 实际需根据已有实现调整
        }
    }

    private void resetCounters() {
        listpacksLen = 0;
        readListpacks = 0;
        producersLen = 0;
        readProducers = 0;
        currentProducerEntries = 0;
        readProducerEntries = 0;
        cgroupsLen = 0;
        readCgroups = 0;
        streamIdStr = null;
        listpackBytes = null;
        producerPid = null;
        entryIid = null;
    }

    @Override
    public boolean isFinish() {
        return state == State.FINISH;
    }

    @Override
    public void reset() {
        super.reset();
        state = State.READ_INIT;
        resetCounters();
        if (stringParser != null) stringParser.reset();
        if (consumerGroupParserInstance != null) consumerGroupParserInstance.reset();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
