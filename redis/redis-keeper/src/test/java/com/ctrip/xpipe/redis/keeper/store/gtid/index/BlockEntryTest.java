package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import io.netty.buffer.ByteBuf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BlockEntryTest {

    private static final String UUID = "test-uuid";

    @Test
    public void testAppendAndSize() throws Exception {
        try (BlockEntry entry = new BlockEntry(UUID, 0, 0)) {
            entry.append(UUID, 1, 10);
            assertEquals(1, entry.getSize());
            entry.append(UUID, 2, 20);
            assertEquals(2, entry.getSize());
            assertEquals(20, entry.getCmdOffset());
            assertEquals(2L, entry.getCurrentGno());
        }
    }

    @Test
    public void testPendingBytesTracksVarIntEncodedBytes() throws Exception {
        try (BlockEntry entry = new BlockEntry(UUID, 0, 0)) {
            int base = entry.getPendingBytes();
            entry.append(UUID, 1, 10);
            assertEquals(base + 1, entry.getPendingBytes());
            entry.append(UUID, 2, 10 + 127);
            assertEquals(base + 2, entry.getPendingBytes());
            // delta > 127 => 2-byte VarInt
            entry.append(UUID, 3, 10 + 127 + 128);
            assertEquals(base + 4, entry.getPendingBytes());
        }
    }

    @Test
    public void testIsGnoGap() throws Exception {
        try (BlockEntry entry = new BlockEntry(UUID, 5, 0)) {
            assertFalse(entry.isGnoGap(UUID, 6));
            assertTrue(entry.isGnoGap(UUID, 8));
            assertTrue(entry.isGnoGap("other-uuid", 6));
        }
    }

    @Test
    public void testIsBlockFull() throws Exception {
        try (BlockEntry entry = new BlockEntry(UUID, 0, 0, 3)) {
            assertFalse(entry.isBlockFull());
            entry.append(UUID, 1, 10);
            entry.append(UUID, 2, 20);
            entry.append(UUID, 3, 30);
            assertTrue(entry.isBlockFull());
        }
    }

    @Test
    public void testNeedChangeBlock() throws Exception {
        try (BlockEntry entry = new BlockEntry(UUID, 0, 0, 4)) {
            entry.append(UUID, 1, 10);
            assertFalse(entry.needChangeBlock(UUID, 2));
            assertTrue(entry.needChangeBlock(UUID, 5));
            assertTrue(entry.needChangeBlock("other-uuid", 2));
        }
    }

    @Test
    public void testDrainToByteBufResetsCacheButKeepsMetadata() throws Exception {
        try (BlockEntry entry = new BlockEntry(UUID, 0, 100)) {
            entry.append(UUID, 1, 110);
            entry.append(UUID, 2, 130);
            int cmdOffsetBefore = entry.getCmdOffset();
            int sizeBefore = entry.getSize();

            ByteBuf drained = entry.drainToByteBuf();
            try {
                assertEquals(2, drained.readableBytes());
                assertEquals(10, drained.readByte() & 0x7f);
                assertEquals(20, drained.readByte() & 0x7f);
            } finally {
                drained.release();
            }

            assertEquals(0, entry.getPendingBytes());
            assertEquals(cmdOffsetBefore, entry.getCmdOffset());
            assertEquals(sizeBefore, entry.getSize());

            entry.append(UUID, 3, 200);
            assertEquals(200, entry.getCmdOffset());
        }
    }

    @Test
    public void testRecoverRecomputesStateFromVarIntStream() throws Exception {
        try (BlockEntry source = new BlockEntry(UUID, 10, 1000)) {
            source.append(UUID, 11, 1050);
            source.append(UUID, 12, 1200);
            source.append(UUID, 13, 1201);
            ByteBuf encoded = source.drainToByteBuf();
            try (BlockEntry recovered = new BlockEntry(UUID, 10, 1000)) {
                recovered.recover(encoded, UUID, 10, 1000);
                assertEquals(3, recovered.getSize());
                assertEquals(1201, recovered.getCmdOffset());
                assertEquals(13L, recovered.getCurrentGno());
                assertEquals(UUID, recovered.getCurrentUuid());
                assertEquals(0, recovered.getPendingBytes());
            } finally {
                encoded.release();
            }
        }
    }

    @Test
    public void testResetStartsFreshBlockOnReusedInstance() throws Exception {
        try (BlockEntry entry = new BlockEntry(UUID, 0, 100)) {
            entry.append(UUID, 1, 110);
            entry.append(UUID, 2, 130);
            assertEquals(2, entry.getSize());
            assertTrue(entry.getPendingBytes() > 0);

            String nextUuid = "next-uuid";
            entry.reset(nextUuid, 20, 500);

            assertEquals(nextUuid, entry.getCurrentUuid());
            assertEquals(20L, entry.getStartGno());
            assertEquals(20L, entry.getCurrentGno());
            assertEquals(500, entry.getCmdOffset());
            assertEquals(0, entry.getSize());
            assertEquals(0, entry.getPendingBytes());
            assertFalse(entry.isGnoGap(nextUuid, 21));

            entry.append(nextUuid, 21, 520);
            assertEquals(1, entry.getSize());
            assertEquals(520, entry.getCmdOffset());
        }
    }

    @Test
    public void testResetMatchesNewInstanceAfterDrain() throws Exception {
        try (BlockEntry reused = new BlockEntry(UUID, 0, 100)) {
            reused.append(UUID, 1, 110);
            ByteBuf drained = reused.drainToByteBuf();
            drained.release();

            String uuid = "other-uuid";
            reused.reset(uuid, 5, 200);

            try (BlockEntry fresh = new BlockEntry(uuid, 5, 200)) {
                fresh.append(uuid, 6, 220);
                reused.append(uuid, 6, 220);

                assertEquals(fresh.getSize(), reused.getSize());
                assertEquals(fresh.getCmdOffset(), reused.getCmdOffset());
                assertEquals(fresh.getCurrentGno(), reused.getCurrentGno());
                assertEquals(fresh.getPendingBytes(), reused.getPendingBytes());
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testAppendAfterCloseThrows() throws Exception {
        BlockEntry entry = new BlockEntry(UUID, 0, 0);
        entry.close();
        entry.append(UUID, 1, 10);
    }
}
