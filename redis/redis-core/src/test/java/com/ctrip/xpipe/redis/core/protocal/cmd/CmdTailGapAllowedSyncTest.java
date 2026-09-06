package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CmdTailGapAllowedSyncTest extends AbstractRedisTest {

    private static final String REPL_ID = "0123456789012345678901234567890123456789";

    private ByteArrayOutputStream received;
    private CmdTailGapAllowedSync sync;

    @Before
    public void beforeCmdTailGapAllowedSyncTest() {
        received = new ByteArrayOutputStream();
        sync = newSync();
    }

    @Test
    public void testGetSyncRequestAlwaysCmdTail() {
        for (int i = 0; i < 3; i++) {
            assertCmdTailRequest(sync.getSyncRequest());
        }

        sync.getRequest().release();
        feedContinue(sync, REPL_ID, 100L);
        assertCmdTailRequest(sync.getSyncRequest());
        assertCmdTailRequest(newSync().getSyncRequest());
    }

    @Test
    public void testCreateRdbReaderThrows() {
        try {
            sync.createRdbReader();
            Assert.fail("expected RedisRuntimeException");
        } catch (RedisRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("createRdbReader"));
        }
    }

    @Test
    public void testDoOnFullSyncThrows() {
        try {
            sync.doOnFullSync();
            Assert.fail("expected RedisRuntimeException");
        } catch (RedisRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("doOnFullSync"));
        }
    }

    @Test
    public void testDoOnXFullSyncThrows() {
        try {
            sync.doOnXFullSync();
            Assert.fail("expected RedisRuntimeException");
        } catch (RedisRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("doOnXFullSync"));
        }
    }

    @Test
    public void testFailReadRdbThrows() {
        try {
            sync.failReadRdb(new IOException("rdb"));
            Assert.fail("expected RedisRuntimeException");
        } catch (RedisRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("failReadRdb"));
            Assert.assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void testFullResyncReceiveFails() {
        sync.getRequest().release();
        ByteBufReceiver.RECEIVER_RESULT result = sync.receive(null, Unpooled.wrappedBuffer(
                ("+FULLRESYNC " + REPL_ID + " 1\r\n").getBytes(StandardCharsets.UTF_8)));
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.FAIL, result);
        Assert.assertTrue(sync.future().isDone());
        Assert.assertFalse(sync.future().isSuccess());
        Assert.assertTrue(sync.future().cause().getMessage().contains("doOnFullSync"));
    }

    @Test
    public void testContinueOffsetUsedAsIsAndCommandsPassThrough() throws Exception {
        AtomicReference<String> seenReplId = new AtomicReference<>();
        AtomicReference<Long> seenOffset = new AtomicReference<>();
        sync.addPsyncObserver(new PsyncObserver() {
            @Override
            public void onFullSync(long masterRdbOffset) {
            }

            @Override
            public void reFullSync() {
            }

            @Override
            public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) {
            }

            @Override
            public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
            }

            @Override
            public void endWriteRdb() {
            }

            @Override
            public void onContinue(String requestReplId, String responseReplId) {
            }

            @Override
            public void onKeeperContinue(String replId, long beginOffset) {
                seenReplId.set(replId);
                seenOffset.set(beginOffset);
            }
        });

        sync.getRequest().release();
        byte[] payload = new byte[]{0x00, 0x01, (byte) 0xff, 'a', 'b', 'c'};

        ByteBufReceiver.RECEIVER_RESULT continueResult = feedContinue(sync, REPL_ID, 100L);
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE, continueResult);
        Assert.assertEquals(REPL_ID, seenReplId.get());
        Assert.assertEquals(Long.valueOf(100L), seenOffset.get());
        Assert.assertFalse(sync.future().isDone());

        ByteBufReceiver.RECEIVER_RESULT cmdResult = sync.receive(null, Unpooled.wrappedBuffer(payload));
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE, cmdResult);
        Assert.assertArrayEquals(payload, received.toByteArray());
    }

    @Test
    public void testDoesNotReferenceStoreTypes() throws Exception {
        File source = new File(
                "src/main/java/com/ctrip/xpipe/redis/core/protocal/cmd/CmdTailGapAllowedSync.java");
        Assert.assertTrue(source.isFile());
        String text = new String(Files.readAllBytes(source.toPath()), StandardCharsets.UTF_8);
        Assert.assertFalse(text.contains("ReplicationStore"));
        Assert.assertFalse(text.contains("ReplicationStoreManager"));
        Assert.assertFalse(text.contains("RdbStore"));
    }

    private CmdTailGapAllowedSync newSync() {
        return new CmdTailGapAllowedSync(null, buf -> {
            byte[] copy = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), copy);
            try {
                received.write(copy);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }, scheduled);
    }

    private static void assertCmdTailRequest(AbstractGapAllowedSync.SyncRequest request) {
        Assert.assertTrue(request instanceof AbstractGapAllowedSync.PsyncRequest);
        AbstractGapAllowedSync.PsyncRequest psync = (AbstractGapAllowedSync.PsyncRequest) request;
        Assert.assertEquals("?", psync.getReplId());
        Assert.assertEquals(Psync.KEEPER_CMD_TAIL_SYNC_OFFSET, psync.getReplOff());
        ByteBuf formatted = request.format();
        try {
            Assert.assertEquals("PSYNC ? -4\r\n", formatted.toString(StandardCharsets.UTF_8));
        } finally {
            formatted.release();
        }
    }

    private static ByteBufReceiver.RECEIVER_RESULT feedContinue(CmdTailGapAllowedSync sync,
                                                                String replId, long offset) {
        return sync.receive(null, Unpooled.wrappedBuffer(
                ("+CONTINUE " + replId + " " + offset + "\r\n").getBytes(StandardCharsets.UTF_8)));
    }
}
