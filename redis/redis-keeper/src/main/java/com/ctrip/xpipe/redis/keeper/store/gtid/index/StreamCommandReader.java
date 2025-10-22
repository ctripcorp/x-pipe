package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandLister;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamCommandParser;
import com.ctrip.xpipe.redis.core.store.ck.CKStore;
import com.ctrip.xpipe.utils.StringUtil;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class StreamCommandReader implements StreamCommandLister {

    private long currentOffset;
    private long lastOffset = -1;

    private DefaultIndexStore defaultIndexStore;

    private StreamCommandParser streamCommandParser;

    private final CKStore ckStore;

    private static final ThreadLocal<List<Object[]>> TX_CONTEXT = new ThreadLocal<>();

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

    public StreamCommandReader(DefaultIndexStore defaultIndexStore, long offset, RedisOpParser opParser) {
        this.defaultIndexStore = defaultIndexStore;
        this.currentOffset = offset;
        this.lastOffset = offset;
        streamCommandParser = new StreamCommandParser(opParser, this);
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

    public void relaseRemainBuf() {
        streamCommandParser.relaseRemainBuf();
    }

    public long getRemainLength() {
        return streamCommandParser.getRemainLength();
    }

    @Override
    public void onCommand(Object[] payload, ByteBuf commandBuf) throws IOException {
        String gtid = readGtid(payload);
        boolean needWrite = true;
        if (!StringUtil.isEmpty(gtid)) {
            // 写索引
            needWrite &= defaultIndexStore.onCommand(gtid, this.lastOffset);
        }
        if(needWrite) {
            // 写cmd文件
            this.currentOffset += commandBuf.readableBytes();
            defaultIndexStore.onFinishParse(commandBuf);
        }
        lastOffset = this.currentOffset;

        publishPayloads(payload);
    }

    private void publishPayloads(Object[] payload){

        ByteArrayOutputStreamPayload byteArrayOutputStreamPayload0 = (ByteArrayOutputStreamPayload) payload[0];
        byte[] bs0 = byteArrayOutputStreamPayload0.getBytes();
        if (Arrays.equals(PING_BYTES, bs0) || Arrays.equals(SELECT_BYTES,bs0)
                ||Arrays.equals(PING_LOWWER_BYTES,bs0) || Arrays.equals(SELECT_LOWWER_BYTES,bs0)) return;

        if (Arrays.equals(MULTI_BYTES, bs0) || Arrays.equals(MULTI_LOWWER_BYTES,bs0)) {
            List<Object[]> payloads = new ArrayList<>();
            payloads.add(payload);
            TX_CONTEXT.set(payloads);
            return;
        }
        List<Object[]> payloads;

        if(Arrays.equals(GTID_BYTES, bs0) || Arrays.equals(GTID_LOWWER_BYTES,bs0)){
            ByteArrayOutputStreamPayload byteArrayOutputStreamPayload3 = (ByteArrayOutputStreamPayload) payload[3];
            byte[] bs3 = byteArrayOutputStreamPayload3.getBytes();
            if(!Arrays.equals(EXEC_BYTES, bs3) && !Arrays.equals(EXEC_LOWWER_BYTES,bs3)){
                payloads = new ArrayList<>(1);
                payloads.add(payload);
                ckStore.sendPayloads(payloads);
            }else {
                payloads = TX_CONTEXT.get();
                if(payloads != null) {
                    payloads.add(payload);
                    try {
                        ckStore.sendPayloads(payloads);
                    } finally {
                        TX_CONTEXT.remove();
                    }
                }
            }
        }else {
            payloads = TX_CONTEXT.get();
            if (payloads != null) {
                payloads.add(payload);
            }
        }

    }
}
