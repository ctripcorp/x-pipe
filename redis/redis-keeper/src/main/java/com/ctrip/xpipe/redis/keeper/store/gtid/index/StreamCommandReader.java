package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.payload.DirectByteBufInOutPayload;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandLister;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandParser;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamTransactionListener;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItem;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItemParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class StreamCommandReader implements StreamCommandLister {

    private long currentOffset;

    private StreamTransactionListener transactionListener;

    private StreamCommandParser streamCommandParser;

    private final int GTID_PAYLOADS_COMMAND_OFFSET = 3;

    private final TransactionContext transactionContext;

    private static final byte[] MULTI_BYTES = new byte[]{'M','U','L','T','I'};
    private static final byte[] GTID_BYTES = new byte[]{'G','T','I','D'};
    private static final byte[] EXEC_BYTES = new byte[]{'E','X','E','C'};

    private static final String EVENT_TYPE = "STREAM";
    private static final String EVENT_MULTI_DUP = "MULTI_DUP";
    private static final String EVENT_MULTI_MISS = "MULTI_MISS";
    private static final String EVENT_OFFSET_MISMATCH = "OFFSET_MISMATCH";
    private int writeCnt = 0;

    private static final Logger logger = LoggerFactory.getLogger(StreamCommandReader.class);

    public StreamCommandReader(StreamTransactionListener transactionListener, long offset) {
        this.transactionListener = transactionListener;
        this.currentOffset = offset;
        streamCommandParser = new StreamCommandParser(this);
        transactionContext = new TransactionContext();
    }

    public void doRead(ByteBuf byteBuf) throws IOException {
        streamCommandParser.doRead(byteBuf);
    }

    public void resetOffset() {
        this.currentOffset = 0;
    }

    private String readGtid(Object[] payload) {
        if(payload == null || payload.length <= 2) {
            return null;
        }
        DirectByteBufInOutPayload gtidPayload = (DirectByteBufInOutPayload) payload[1];
        return gtidPayload.toString();
    }

    public void resetParser() {
        streamCommandParser.reset();
        if (transactionContext.isActive()) {
            transactionContext.clear();
        }
    }

    public int getRemainLength() {
        return streamCommandParser.getRemainLength();
    }

    public boolean isTransactionActive() {
        return transactionContext.isActive();
    }

    public int getTransactionSize() {
        return transactionContext.size();
    }

    public long getTransactionStartOffset() {
        return transactionContext.getStartOffset();
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        if(payload == null) return;

        DirectByteBufInOutPayload commandPayload = (DirectByteBufInOutPayload) payload[0];

        if (isMultiCommand(commandPayload)) {
            handleMultiCommand(payload, commandBuf);
        } else if (isGtidCommand(commandPayload)) {
            String gtid = readGtid(payload);
            String uuid = gtid.substring(0,40);
            long gno = Long.parseLong(gtid.substring(41));
            handleGtidCommand(uuid,gno, payload, commandBuf);
        } else {
            handleRegularCommand(payload, commandBuf);
        }
    }

    RedisOpItem parsePayload(Object[] payload) {
        return RedisOpItemParser.parse(transactionListener.getOpParser(), payload);
    }

    private void handleMultiCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        if (transactionContext.isActive()) {
            logger.warn("[handleMultiCommand] nested MULTI detected, clearing previous transaction");
            EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_MULTI_DUP);
            transactionContext.clear();
        }

        transactionContext.start(this.currentOffset);
        transactionContext.addCommand(payload, commandBuf);
    }

    private void handleGtidCommand(String uuid,long gno, Object[] payload, ByteBuf commandBuf) throws IOException {
        if (gno < 0) {
            handleRegularCommand(payload, commandBuf);
            return;
        }

        DirectByteBufInOutPayload execPayload = (DirectByteBufInOutPayload) payload[GTID_PAYLOADS_COMMAND_OFFSET];
        boolean isExec = isExecCommand(execPayload);

        if (isExec && transactionContext.isActive()) {
            commitTransaction(uuid,gno, payload, commandBuf);
        } else if (isExec && !transactionContext.isActive()) {
            logger.warn("[handleGtidCommand] EXEC without MULTI, treating as regular command");
            EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_MULTI_MISS);
            processSingleGtidCommand(uuid,gno, payload, commandBuf);
        } else {
            processSingleGtidCommand(uuid,gno, payload, commandBuf);
        }
    }

    private void handleRegularCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        if (transactionContext.isActive()) {
            DirectByteBufInOutPayload commandPayload = (DirectByteBufInOutPayload) payload[0];
            if (isExecCommand(commandPayload)) {
                commitTransaction(null, -1, payload, commandBuf);
            } else {
                transactionContext.addCommand(payload, commandBuf);
            }
        } else {
            writeSingleCommand(commandBuf, payload, null,-1);
        }
    }

    private void processSingleGtidCommand(String uuid,long gno, Object[] payload, ByteBuf commandBuf) throws IOException {
        if (transactionListener.preAppend(uuid,gno)) {
            writeSingleCommand(commandBuf, payload,uuid,gno);
        }
    }

    private boolean isMultiCommand(DirectByteBufInOutPayload commandPayload) {
        return commandPayload != null && commandPayload.equalsIgnoreCaseAsciiExpectedUppercase(MULTI_BYTES);
    }

    private boolean isGtidCommand(DirectByteBufInOutPayload commandPayload) {
        return commandPayload != null && commandPayload.equalsIgnoreCaseAsciiExpectedUppercase(GTID_BYTES);
    }

    private boolean isExecCommand(DirectByteBufInOutPayload commandPayload) {
        return commandPayload != null && commandPayload.equalsIgnoreCaseAsciiExpectedUppercase(EXEC_BYTES);
    }

    private void commitTransaction(String uuid,long gno, Object[] payload, ByteBuf execCommandBuf) throws IOException {
        transactionContext.setUuid(uuid);
        transactionContext.setGno(gno);
        transactionContext.addCommand(payload, execCommandBuf);

        try {
            transactionContext.commit(transactionListener, this);
        } catch (IOException e) {
            transactionContext.clear();
            throw e;
        }
    }

    private void writeSingleCommand(ByteBuf commandBuf, Object[] payload,String uuid,long gno) throws IOException {
        long start = this.currentOffset;
        int cmdLen = commandBuf.readableBytes();
        RedisOpItem redisOpItem = parsePayload(payload);
        transactionListener.postAppend(uuid,gno, start, commandBuf, redisOpItem);
        this.currentOffset += cmdLen;
        mayCheckOffset();
    }

    private void writeMultiCommand(String uuid,long gno, long startOffset, List<ByteBuf> commandBufs, List<RedisOpItem> redisOpItems) throws IOException {
        int totalLen = 0;
        for (ByteBuf commandBuf : commandBufs) {
            if (commandBuf != null) {
                totalLen += commandBuf.readableBytes();
            }
        }
        transactionListener.batchPostAppend(uuid,gno, startOffset, commandBufs, redisOpItems);
        this.currentOffset += totalLen;
        mayCheckOffset();
    }

    private void mayCheckOffset() {
        if (writeCnt++ % 8192 == 0) {
            if (!transactionListener.checkOffset(this.currentOffset)) {
                EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_OFFSET_MISMATCH);
            }
        }
    }

    private class TransactionContext {
        private List<RedisOpItem> payloads = new ArrayList<>();
        private final List<ByteBuf> commandBufs = new ArrayList<>();
        private boolean active = false;
        private long startOffset = -1;
        private String uuid = null;
        private long gno = -1;

        public void start(long offset) {
            clear();
            active = true;
            startOffset = offset;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public void setGno(long gno){
            this.gno = gno;
        }

        public void addCommand(Object[] payload, ByteBuf commandBuf) {
            if (!active) {
                throw new IllegalStateException("Transaction not active");
            }
            if (commandBuf != null) {
                commandBuf.retain();
            }
            payloads.add(StreamCommandReader.this.parsePayload(payload));
            commandBufs.add(commandBuf);
        }

        public void commit(StreamTransactionListener transactionListener, StreamCommandReader reader) throws IOException {
            if (!active) {
                return;
            }

            try {
                long transactionStartOffset = startOffset > -1 ? startOffset : reader.currentOffset;

                if (gno > 0) {
                    if (transactionListener.preAppend(uuid,gno)) {
                        reader.writeMultiCommand(uuid,gno, transactionStartOffset, commandBufs, payloads);
                    }
                } else {
                    reader.writeMultiCommand(null, -1,transactionStartOffset, commandBufs, payloads);
                }
            } finally {
                clear();
            }
        }

        public void clear() {
            for (ByteBuf buf : commandBufs) {
                if (buf != null && buf.refCnt() > 0) {
                    buf.release();
                }
            }
            commandBufs.clear();
            payloads = new ArrayList<>();
            active = false;
            startOffset = -1;
            uuid = null;
            gno = -1;
        }

        public boolean isActive() {
            return active;
        }

        public int size() {
            return commandBufs.size();
        }

        public long getStartOffset() {
            return startOffset;
        }
    }
}
