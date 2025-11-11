package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandLister;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamCommandReader implements StreamCommandLister {

    private long currentOffset;

    private DefaultIndexStore defaultIndexStore;

    private StreamCommandParser streamCommandParser;

    private final int GTID_PAYLOADS_COMMAND_OFFSET = 3;

    private final TransactionContext transactionContext = new TransactionContext();

    private static final byte[] MULTI_BYTES = new byte[]{'M','U','L','T','I'};
    private static final byte[] GTID_BYTES = new byte[]{'G','T','I','D'};
    private static final byte[] EXEC_BYTES = new byte[]{'E','X','E','C'};

    private static final byte[] MULTI_LOWWER_BYTES = new byte[]{'m','u','l','t','i'};
    private static final byte[] GTID_LOWWER_BYTES = new byte[]{'g','t','i','d'};
    private static final byte[] EXEC_LOWWER_BYTES = new byte[]{'e','x','e','c'};

    private static final String EVENT_TYPE = "STREAM";
    private static final String EVENT_MULTI_DUP = "MULTI_DUP";
    private static final String EVENT_MULTI_MISS = "MULTI_MISS";
    private static final String EVENT_OFFSET_MISMATCH = "OFFSET_MISMATCH";
    private int writeCnt = 0;

    private static final Logger logger = LoggerFactory.getLogger(StreamCommandReader.class);

    public StreamCommandReader(DefaultIndexStore defaultIndexStore, long offset) {
        this.defaultIndexStore = defaultIndexStore;
        this.currentOffset = offset;
        streamCommandParser = new StreamCommandParser(this);
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
        if(StringUtil.trimEquals("GTID", payload[0].toString())) {
            return payload[1].toString();
        } else {
            return null;
        }

    }

    public void resetParser() {
        streamCommandParser.reset();
        // Clear transaction state
        if (transactionContext.isActive()) {
            transactionContext.clear();
        }
    }

    public int getRemainLength() {
        return streamCommandParser.getRemainLength();
    }

    /**
     * Check if there is an active transaction
     */
    public boolean isTransactionActive() {
        return transactionContext.isActive();
    }

    /**
     * Get the number of commands in the transaction
     */
    public int getTransactionSize() {
        return transactionContext.size();
    }

    /**
     * Get the start offset of the active transaction
     * @return the start offset, or -1 if no transaction is active
     */
    public long getTransactionStartOffset() {
        return transactionContext.getStartOffset();
    }

    /**
     * Get the current offset
     * @return the current offset
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        if(payload == null) return;

        ByteArrayOutputStreamPayload byteArrayOutputStreamPayload0 = (ByteArrayOutputStreamPayload) payload[0];
        byte[] bs0 = byteArrayOutputStreamPayload0.getBytes();

        // Process by priority: MULTI > GTID > regular commands
        if (isMultiCommand(bs0)) {
            handleMultiCommand(payload, commandBuf);
        } else if (isGtidCommand(bs0)) {
            String gtid = readGtid(payload);
            handleGtidCommand(gtid, payload, commandBuf);
        } else {
            handleRegularCommand(payload, commandBuf);
        }
    }

    private void handleMultiCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        // If transaction already exists, clear it first (may be an abnormal situation)
        if (transactionContext.isActive()) {
            // Log warning but don't throw exception, continue processing
            logger.warn("[handleMultiCommand] nested MULTI detected, clearing previous transaction");
            EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_MULTI_DUP);
            transactionContext.clear();
        }

        // Start new transaction, record current offset (offset when transaction starts)
        transactionContext.start(this.currentOffset);
        // Add MULTI command to transaction (don't update offset, update uniformly when transaction commits)
        transactionContext.addCommand(payload, commandBuf);
        // Note: MULTI command's offset is not updated immediately, process uniformly when transaction commits
    }

    private void handleGtidCommand(String gtid, Object[] payload, ByteBuf commandBuf) throws IOException {
        if (StringUtil.isEmpty(gtid)) {
            // Not GTID format, process as regular command
            handleRegularCommand(payload, commandBuf);
            return;
        }

        // Check if it is an EXEC command
        ByteArrayOutputStreamPayload execPayload = (ByteArrayOutputStreamPayload) payload[GTID_PAYLOADS_COMMAND_OFFSET];
        byte[] execBytes = execPayload.getBytes();
        boolean isExec = isExecCommand(execBytes);

        if (isExec && transactionContext.isActive()) {
            // Transaction commit: GTID + EXEC
            commitTransaction(gtid, payload, commandBuf);
        } else if (isExec && !transactionContext.isActive()) {
            // EXEC but no MULTI, may be an error state, process as regular command
            logger.warn("[handleGtidCommand] EXEC without MULTI, treating as regular command");
            EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_MULTI_MISS);
            processSingleGtidCommand(gtid, payload, commandBuf);
        } else {
            // Regular GTID command (not in transaction)
            processSingleGtidCommand(gtid, payload, commandBuf);
        }
    }

    private void handleRegularCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        if (transactionContext.isActive()) {
            // Command in transaction (don't update offset, update uniformly when transaction commits)
            transactionContext.addCommand(payload, commandBuf);
        } else {
            // Regular command, write directly
            writeSingleCommand(commandBuf);
        }
    }

    private void processSingleGtidCommand(String gtid, Object[] payload, ByteBuf commandBuf) throws IOException {
        long offset = this.currentOffset;
        if (defaultIndexStore.onCommand(gtid, offset)) {
            writeSingleCommand(commandBuf);
        }
    }

    private boolean isMultiCommand(byte[] command) {
        return Arrays.equals(MULTI_BYTES, command) ||
                Arrays.equals(MULTI_LOWWER_BYTES, command);
    }

    private boolean isGtidCommand(byte[] command) {
        return Arrays.equals(GTID_BYTES, command) ||
                Arrays.equals(GTID_LOWWER_BYTES, command);
    }

    private boolean isExecCommand(byte[] command) {
        return Arrays.equals(EXEC_BYTES, command) ||
                Arrays.equals(EXEC_LOWWER_BYTES, command);
    }

    private void commitTransaction(String gtid, Object[] payload, ByteBuf execCommandBuf) throws IOException {
        if (!transactionContext.isActive()) {
            // No active transaction, process as regular command
            processSingleGtidCommand(gtid, payload, execCommandBuf);
            return;
        }

        // Set transaction's GTID (using offset when transaction starts)
        transactionContext.setGtid(gtid);

        // Add EXEC command to transaction
        transactionContext.addCommand(payload, execCommandBuf);

        try {
            // Commit transaction: write index and all commands (offset update is completed in commit method)
            transactionContext.commit(defaultIndexStore, this);
        } catch (IOException e) {
            // Transaction commit failed, clear state
            transactionContext.clear();
            throw e;
        }
    }

    private void writeSingleCommand(ByteBuf commandBuf) throws IOException {
        int cmdLen = commandBuf.readableBytes();
        defaultIndexStore.onFinishParse(commandBuf);
        this.currentOffset += cmdLen;

        if (writeCnt++ % 8192 == 0) {
            // check offset match
            long cmdFileLen = defaultIndexStore.getCurrentCmdFileLen();
            if (-1 != cmdFileLen && cmdFileLen != this.currentOffset) {
                logger.info("[checkOffset][mismatch] nextCmdBegin:{} cmdFileLen{}", this.currentOffset, cmdFileLen);
                EventMonitor.DEFAULT.logEvent(EVENT_TYPE, EVENT_OFFSET_MISMATCH);
            }
        }
    }

    private static class TransactionContext {
        private final List<Object[]> payloads = new ArrayList<>();
        private final List<ByteBuf> commandBufs = new ArrayList<>();
        private boolean active = false;
        private long startOffset = -1;
        private String gtid = null;

        public void start(long offset) {
            clear();
            active = true;
            startOffset = offset;
        }

        public void setGtid(String gtid) {
            this.gtid = gtid;
        }

        public void addCommand(Object[] payload, ByteBuf commandBuf) {
            if (!active) {
                throw new IllegalStateException("Transaction not active");
            }
            // Retain ByteBuf reference to ensure it's not released
            if (commandBuf != null) {
                commandBuf.retain();
            }
            payloads.add(payload);
            commandBufs.add(commandBuf);
        }

        public void commit(DefaultIndexStore indexStore, StreamCommandReader reader) throws IOException {
            if (!active) {
                return;
            }

            try {
                // Calculate offset of first command (MULTI) in transaction
                // startOffset is the last offset before transaction starts, so MULTI's offset is startOffset
                // But for more accuracy, we should use currentOffset (if updated) or startOffset
                long transactionStartOffset = startOffset > -1 ? startOffset : reader.currentOffset;

                // Write index using transaction's GTID and first command's offset
                if (gtid != null && !StringUtil.isEmpty(gtid)) {
                    boolean indexWritten = indexStore.onCommand(gtid, transactionStartOffset);
                    if (indexWritten) {
                        // Write all commands and update offset
                        for (ByteBuf buf : commandBufs) {
                            if (buf != null) {
                                reader.writeSingleCommand(buf);
                            }
                        }
                    }
                } else {
                    // No GTID, write commands directly
                    for (ByteBuf buf : commandBufs) {
                        if (buf != null) {
                            reader.writeSingleCommand(buf);
                        }
                    }
                }
            } finally {
                clear();
            }
        }

        public void clear() {
            // Release all ByteBuf
            for (ByteBuf buf : commandBufs) {
                if (buf != null && buf.refCnt() > 0) {
                    buf.release();
                }
            }
            payloads.clear();
            commandBufs.clear();
            active = false;
            startOffset = -1;
            gtid = null;
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

        public String getGtid() {
            return gtid;
        }
    }

}








