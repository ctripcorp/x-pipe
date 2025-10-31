package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandLister;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandParser;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamCommandReader implements StreamCommandLister {

    private long currentOffset;
    private long lastOffset = -1;

    private DefaultIndexStore defaultIndexStore;

    private StreamCommandParser streamCommandParser;

    private final CKStore ckStore;

    private final int GTID_PAYLOADS_COMMAND_OFFSET = 3;

    private static final ThreadLocal<List<Object[]>> TX_CONTEXT = new ThreadLocal<>();
    private static final ThreadLocal<List<ByteBuf>> TX_BYTEBUF_CONTEXT = new ThreadLocal<>();

    private static final byte[] MULTI_BYTES = new byte[]{'M','U','L','T','I'};
    private static final byte[] GTID_BYTES = new byte[]{'G','T','I','D'};
    private static final byte[] EXEC_BYTES = new byte[]{'E','X','E','C'};

    private static final byte[] MULTI_LOWWER_BYTES = new byte[]{'m','u','l','t','i'};
    private static final byte[] GTID_LOWWER_BYTES = new byte[]{'g','t','i','d'};
    private static final byte[] EXEC_LOWWER_BYTES = new byte[]{'e','x','e','c'};

    private static final byte[] PING_LOWWER_BYTES = new byte[]{'p','i','n','g'};
    private static final byte[] PING_BYTES = new byte[]{'P','I','N','G'};
    private static final byte[] SELECT_BYTES = new byte[]{'S','E','L','E','C','T'};
    private static final byte[] SELECT_LOWWER_BYTES = new byte[]{'s','e','l','e','c','t'};

    public StreamCommandReader(DefaultIndexStore defaultIndexStore, long offset) {
        this.defaultIndexStore = defaultIndexStore;
        this.currentOffset = offset;
        this.lastOffset = offset;
        streamCommandParser = new StreamCommandParser(this);
        this.ckStore = defaultIndexStore.getCkStore();
    }

    public void doRead(ByteBuf byteBuf) throws IOException {
        streamCommandParser.doRead(byteBuf);
    }

    public void resetOffset() {
        this.currentOffset = 0;
        this.lastOffset = 0;
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
    }

    public int getRemainLength() {
        return streamCommandParser.getRemainLength();
    }

    @Override
    public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        if(payload == null) return;

        ByteArrayOutputStreamPayload byteArrayOutputStreamPayload0 = (ByteArrayOutputStreamPayload) payload[0];
        byte[] bs0 = byteArrayOutputStreamPayload0.getBytes();

        if (isGtidCommand(bs0)) {
            String gtid = readGtid(payload);
            handleGtidCommand(gtid,payload, commandBuf);
            lastOffset = this.currentOffset;
            return;
        }else {
            if(TX_CONTEXT.get() == null && !isMultiCommand(bs0)){
                writeCommandBufs(Arrays.asList(commandBuf));
                lastOffset = this.currentOffset;
            }
        }

        if (isMultiCommand(bs0)) {
            clearTransactionContext();
            List<Object[]> payloads = new ArrayList<>();
            payloads.add(payload);
            TX_CONTEXT.set(payloads);

            List<ByteBuf> commandBufs = new ArrayList<>();
            commandBufs.add(commandBuf);
            TX_BYTEBUF_CONTEXT.set(commandBufs);
            return;
        }

        processTransactionCommand(payload, commandBuf);
    }

    private boolean isPingOrSelectCommand(byte[] command) {
        return Arrays.equals(PING_BYTES, command) ||
                Arrays.equals(SELECT_BYTES, command) ||
                Arrays.equals(PING_LOWWER_BYTES, command) ||
                Arrays.equals(SELECT_LOWWER_BYTES, command);
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

    private void clearTransactionContext() {
        TX_CONTEXT.remove();
        TX_BYTEBUF_CONTEXT.remove();
    }

    private void handleGtidCommand(String gtid,Object[] payload, ByteBuf commandBuf) throws IOException {
        if (StringUtil.isEmpty(gtid) || !defaultIndexStore.onCommand(gtid, this.lastOffset)) {
            return;
        }

        ByteArrayOutputStreamPayload byteArrayOutputStreamPayload3 = (ByteArrayOutputStreamPayload) payload[GTID_PAYLOADS_COMMAND_OFFSET];
        byte[] bs3 = byteArrayOutputStreamPayload3.getBytes();

        if (!isExecCommand(bs3)) {
            processSingleCommand(payload, commandBuf);
        } else {
            commitTransaction(payload, commandBuf);
        }
    }

    private void processSingleCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        List<Object[]> payloads = new ArrayList<>(1);
        payloads.add(payload);

        List<ByteBuf> commandBufs = new ArrayList<>(1);
        commandBufs.add(commandBuf);

        writeCommandBufs(commandBufs);
        if (ckStore != null && !ckStore.isKeeper()) {
            ckStore.sendPayloads(payloads);
        }
    }

    private void commitTransaction(Object[] payload, ByteBuf commandBuf) throws IOException {
        List<Object[]> payloads = TX_CONTEXT.get();
        List<ByteBuf> commandBufs = TX_BYTEBUF_CONTEXT.get();

        if (commandBufs != null) {
            commandBufs.add(commandBuf);
            try {
                writeCommandBufs(commandBufs);
            } finally {
                TX_BYTEBUF_CONTEXT.remove();
            }
        }

        if (payloads != null) {
            payloads.add(payload);
            try {
                if (ckStore != null && !ckStore.isKeeper()) {
                    ckStore.sendPayloads(payloads);
                }
            } finally {
                TX_CONTEXT.remove();
            }
        }
    }

    private void processTransactionCommand(Object[] payload, ByteBuf commandBuf) {
        List<Object[]> payloads = TX_CONTEXT.get();
        if (payloads != null) {
            payloads.add(payload);
        }

        List<ByteBuf> commandBufs = TX_BYTEBUF_CONTEXT.get();
        if (commandBufs != null) {
            commandBufs.add(commandBuf);
        }
    }

    private void writeCommandBufs(List<ByteBuf> commandBufs) throws IOException{
        // 写cmd文件
        for (ByteBuf byteBuf : commandBufs){
            this.currentOffset += byteBuf.readableBytes();
            defaultIndexStore.onFinishParse(byteBuf);
        }
    }

}
