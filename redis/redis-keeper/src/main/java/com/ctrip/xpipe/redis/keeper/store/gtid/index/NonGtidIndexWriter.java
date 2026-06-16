package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * 非 GTID 区间索引：每条记录 16 字节 = start(long) + end(long)。
 * 仅追加写，无原地更新。线程安全由调用方（DefaultIndexStore 串行写路径）保证。
 */
public class NonGtidIndexWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(NonGtidIndexWriter.class);

    public static final String ZONE_PREFIX = "nongtid_";
    static final int RECORD_LEN = 16;
    static final int FLUSH_THRESHOLD = 8192;

    private final String baseDir;
    private final String cmdFileName;
    private ControllableFile zoneFile;

    private long zoneStart = -1L;
    private long zoneEnd = 0L;
    private int zoneCmdCount = 0;

    public NonGtidIndexWriter(String baseDir, String cmdFileName) {
        this.baseDir = baseDir;
        this.cmdFileName = cmdFileName;
    }

    public void init() throws IOException {
        this.zoneFile = new DefaultControllableFile(new File(generateZoneFileName()));
        FileChannel ch = zoneFile.getFileChannel();
        long size = ch.size();
        long aligned = (size / RECORD_LEN) * RECORD_LEN;
        if (aligned != size) {
            log.warn("[NonGtidIndexWriter] truncate torn record from {} to {} on {}",
                    size, aligned, cmdFileName);
            ch.truncate(aligned);
        }
        ch.position(aligned);
        log.info("[NonGtidIndexWriter] init {} bytes={}", cmdFileName, aligned);
    }

    public void appendNonGtid(long offset, int length) throws IOException {
        if (zoneStart == -1L) {
            zoneStart = offset;
        }
        zoneEnd = offset + length;
        if (++zoneCmdCount >= FLUSH_THRESHOLD) {
            flushCurrentZone();
        }
    }

    public void onGtid() throws IOException {
        flushCurrentZone();
    }

    @Override
    public void close() throws IOException {
        try {
            flushCurrentZone();
        } finally {
            if (zoneFile != null) {
                zoneFile.close();
                zoneFile = null;
            }
        }
    }

    /**
     * 加载所有已落盘的 zone 区间（升序）。撕裂尾部记录在 init 时已被裁掉。
     */
    public List<long[]> loadAllZones() throws IOException {
        if (zoneFile == null) return new ArrayList<>();
        FileChannel ch = zoneFile.getFileChannel();
        long size = ch.size();
        if (size < RECORD_LEN) return new ArrayList<>();

        ByteBuffer buf = ByteBuffer.allocate((int) size);
        ch.position(0);
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n < 0) break;
        }
        ch.position(ch.size());
        buf.flip();

        List<long[]> zones = new ArrayList<>((int) (size / RECORD_LEN));
        while (buf.remaining() >= RECORD_LEN) {
            long s = buf.getLong();
            long e = buf.getLong();
            if (s >= 0 && e > s) {
                zones.add(new long[]{s, e});
            } else {
                log.warn("[NonGtidIndexWriter] skip invalid record [{}, {}) on {}", s, e, cmdFileName);
            }
        }
        return zones;
    }

    public boolean hasActiveZone() {
        return zoneStart != -1L;
    }

    String generateZoneFileName() {
        return Paths.get(baseDir, ZONE_PREFIX + cmdFileName).toString();
    }

    public static void deleteZoneFiles(String baseDir) {
        File dir = new File(baseDir);
        if (!dir.isDirectory()) return;
        File[] files = dir.listFiles((d, name) -> name.startsWith(ZONE_PREFIX));
        if (files == null) return;
        for (File f : files) f.delete();
    }

    private void flushCurrentZone() throws IOException {
        if (zoneStart == -1L) return;
        ByteBuffer buf = ByteBuffer.allocate(RECORD_LEN);
        buf.putLong(zoneStart);
        buf.putLong(zoneEnd);
        buf.flip();
        FileChannel ch = zoneFile.getFileChannel();
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
        if (log.isDebugEnabled()) {
            log.debug("[NonGtidIndexWriter] flushed [{}, {}) cnt={} on {}",
                    zoneStart, zoneEnd, zoneCmdCount, cmdFileName);
        }
        zoneStart = -1L;
        zoneEnd = 0L;
        zoneCmdCount = 0;
    }
}
