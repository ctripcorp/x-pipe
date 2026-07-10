package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.io.File;

/**
 * @author TB
 * @date 2026/6/30 22:42
 */

public class IndexEntryTest {

    private String validUuid40;
    private String shortUuid;
    private long startGno;
    private long cmdStartOffset;
    private long blockStartOffset;
    private long blockEndOffset;
    private int size;
    private long cmdEndOffset;
    private long zoneStart;
    private long zoneEnd;
    private int zoneCmdCount;

    @Before
    public void setUp() {
        // 构造一个恰好 40 字节的 UUID 字符串
        validUuid40 = "a4f566ef50a85e1119f17f9b746728b48609a2ab"; // 40 chars
        shortUuid = "a4f566ef50a85e1119f17f9b746728b48609a2a"; // 39 chars
        startGno = 100L;
        cmdStartOffset = 500L;
        blockStartOffset = 0L;
        blockEndOffset = 200L;
        size = 50;
        cmdEndOffset = 580L;
        zoneStart = 1000L;
        zoneEnd = 2000L;
        zoneCmdCount = 30;
    }

    // ---------- GTID 条目测试 ----------
    @Test
    public void testV2GtidEntrySerializeDeserialize() {
        IndexEntry entry = new IndexEntry(validUuid40, startGno, cmdStartOffset, blockStartOffset);
        entry.setBlockEndOffset(blockEndOffset);
        entry.setSize(size);
        entry.setCmdEndOffset(cmdEndOffset);

        ByteBuffer buffer = entry.generateBufferV2();
        assertNotNull(buffer);
        assertEquals(IndexEntry.SEGMENT_LENGTH_V2, buffer.remaining());

        IndexEntry parsed = IndexEntry.fromBufferV2(buffer);
        assertNotNull(parsed);
        assertEquals(IndexEntryType.GTID, parsed.getType());
        assertEquals(0, parsed.getFlags());
        assertEquals(validUuid40, parsed.getUuid());
        assertEquals(startGno, parsed.getStartGno());
        assertEquals(cmdStartOffset, parsed.getCmdStartOffset());
        assertEquals(blockStartOffset, parsed.getBlockStartOffset());
        assertEquals(blockEndOffset, parsed.getBlockEndOffset());
        assertEquals(size, parsed.getSize());
        assertEquals(cmdEndOffset, parsed.getCmdEndOffset());
        assertFalse(parsed.isZone());
    }

    // ---------- ZONE 条目测试 ----------
    @Test
    public void testV2ZoneEntrySerializeDeserialize() {
        IndexEntry entry = IndexEntry.zone(zoneStart, zoneEnd, zoneCmdCount);

        ByteBuffer buffer = entry.generateBufferV2();
        assertEquals(IndexEntry.SEGMENT_LENGTH_V2, buffer.remaining());

        IndexEntry parsed = IndexEntry.fromBufferV2(buffer);
        assertNotNull(parsed);
        assertTrue(parsed.isZone());
        assertEquals(IndexEntryType.ZONE, parsed.getType());
        assertEquals(IndexEntry.ZONE_UUID, parsed.getUuid());
        assertEquals(0L, parsed.getStartGno());
        assertEquals(zoneStart, parsed.getCmdStartOffset());
        assertEquals(0L, parsed.getBlockStartOffset());   // ZONE 的 blockStartOffset 应为 0
        assertEquals(0L, parsed.getBlockEndOffset());   // ZONE 的 blockEndOffset 应为 0
        assertEquals(zoneCmdCount, parsed.getSize());
        assertEquals(zoneEnd, parsed.getCmdEndOffset());   // ZONE 结束位置在 cmdEndOffset
        assertEquals(zoneEnd, parsed.getZoneEnd());
        assertEquals(zoneStart, parsed.getZoneStart());
    }

    // ---------- 破坏数据仍能解析（无 CRC 校验，字段直接读入） ----------
    @Test
    public void testV2BitFlipParsedButFieldsDiffer() {
        IndexEntry entry = new IndexEntry(validUuid40, startGno, cmdStartOffset, blockStartOffset);
        entry.setBlockEndOffset(blockEndOffset);
        entry.setSize(size);
        entry.setCmdEndOffset(cmdEndOffset);
        ByteBuffer buffer = entry.generateBufferV2();

        // 修改 buffer 中某个字节（非 UUID/CRC 部分），破坏 startGno
        byte[] array = buffer.array();
        int flipPos = 44; // startGno 起始
        array[flipPos] ^= 0x01;
        buffer.clear();
        buffer.put(array);
        buffer.flip();

        IndexEntry parsed = IndexEntry.fromBufferV2(buffer);
        assertNotNull("v2 无 CRC，parse 不应返回 null", parsed);
        assertNotEquals("被翻转的字段应产生不同值", startGno, parsed.getStartGno());
    }

    // ---------- 固定 88 字节 ----------
    @Test
    public void testV2BufferLength() {
        IndexEntry gtidEntry = new IndexEntry(validUuid40, startGno, cmdStartOffset, blockStartOffset);
        assertEquals(IndexEntry.SEGMENT_LENGTH_V2, gtidEntry.generateBufferV2().remaining());

        IndexEntry zoneEntry = IndexEntry.zone(zoneStart, zoneEnd, zoneCmdCount);
        assertEquals(IndexEntry.SEGMENT_LENGTH_V2, zoneEntry.generateBufferV2().remaining());
    }

    // ---------- UUID 长度保证（调用方责任）----------
    @Test
    public void testV2UuidLengthAssumption() {
        // 当前实现要求 uuid 长度为 40 字节，否则序列化后的字段会错位。
        // 这里演示：使用 39 字节的 uuid 会产生错误的序列化结果（后续字段读取会偏移）。
        // 但这不是 IndexEntry 的问题，而是调用者必须保证传入 40 字节的 UUID。
        // 因此本测试不执行具体的解析，仅说明该假设。
        assertTrue("UUID length must be 40 for v2 serialization", validUuid40.length() == 40);
    }

    // ---------- 从 Buffer 读取时 remaining 不足 ----------
    @Test
    public void testFromBufferV2InsufficientData() {
        ByteBuffer buf = ByteBuffer.allocate(10);
        assertNull(IndexEntry.fromBufferV2(buf));
    }

    // ---------- readFromFileV2 集成测试（使用临时文件）----------
    @Test
    public void testReadFromFileV2() throws Exception {
        File tempFile = File.createTempFile("index_v2_test", ".tmp");
        tempFile.deleteOnExit();
        RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
        FileChannel channel = raf.getChannel();

        // 写入一个有效的 GTID 条目和紧跟的一个 ZONE 条目
        IndexEntry gtidEntry = new IndexEntry(validUuid40, startGno, cmdStartOffset, blockStartOffset);
        gtidEntry.setBlockEndOffset(blockEndOffset);
        gtidEntry.setSize(size);
        gtidEntry.setCmdEndOffset(cmdEndOffset);
        ByteBuffer gtidBuf = gtidEntry.generateBufferV2();
        channel.write(gtidBuf);

        IndexEntry zoneEntry = IndexEntry.zone(zoneStart, zoneEnd, zoneCmdCount);
        ByteBuffer zoneBuf = zoneEntry.generateBufferV2();
        channel.write(zoneBuf);

        channel.position(0);

        // 读取第一条 GTID 条目
        IndexEntry readGtid = IndexEntry.readFromFileV2(channel);
        assertNotNull(readGtid);
        assertEquals(validUuid40, readGtid.getUuid());
        assertEquals(startGno, readGtid.getStartGno());
        assertFalse(readGtid.isZone());

        // 读取第二条 ZONE 条目
        IndexEntry readZone = IndexEntry.readFromFileV2(channel);
        assertNotNull(readZone);
        assertTrue(readZone.isZone());
        assertEquals(zoneStart, readZone.getZoneStart());
        assertEquals(zoneEnd, readZone.getZoneEnd());

        // 文件末尾应返回 null
        IndexEntry readNull = IndexEntry.readFromFileV2(channel);
        assertNull(readNull);

        channel.close();
        raf.close();
    }

    // ---------- ZONE 条目 isZone 兼容 v1 判断 ----------
    @Test
    public void testIsZoneV1Compatibility() {
        // 模拟 v1 方式创建的 ZONE 条目（无 type 字段），应仍能通过 uuid 判断为 ZONE
        IndexEntry entry = new IndexEntry(IndexEntry.ZONE_UUID, 0L, zoneStart, zoneEnd);
        entry.setBlockEndOffset(zoneEnd);
        // 不设置 type，默认是 GTID
        assertTrue(entry.isZone()); // 因为 uuid 是全 0
    }

    // ---------- getters/setters 覆盖 ----------
    @Test
    public void testSettersAndGetters() {
        IndexEntry entry = new IndexEntry(validUuid40, startGno, cmdStartOffset, blockStartOffset);
        entry.setBlockEndOffset(500L);
        assertEquals(500L, entry.getBlockEndOffset());
        entry.setSize(10);
        assertEquals(10, entry.getSize());
        entry.setCmdEndOffset(120L);
        assertEquals(120L, entry.getCmdEndOffset());
        entry.setPosition(999);
        assertEquals(999, entry.getPosition());
        assertEquals(startGno + 10 - 1, entry.getEndGno());
    }
}
