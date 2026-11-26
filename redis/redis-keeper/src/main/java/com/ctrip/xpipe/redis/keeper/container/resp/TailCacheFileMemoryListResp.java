package com.ctrip.xpipe.redis.keeper.container.resp;

import com.ctrip.xpipe.redis.keeper.storage.TailCacheFileMemoryStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TailCacheFileMemoryListResp {

    private final boolean initialized;
    private final long snapshotTimeMillis;
    private final int total;
    private final boolean truncated;
    private final List<Item> files;

    private TailCacheFileMemoryListResp(boolean initialized, long snapshotTimeMillis, int total, List<Item> files) {
        this.initialized = initialized;
        this.snapshotTimeMillis = snapshotTimeMillis;
        this.total = total;
        this.truncated = total > files.size();
        this.files = files;
    }

    // rows: already sorted and limited; total: row count before the limit
    public static TailCacheFileMemoryListResp from(List<TailCacheFileMemoryStats> rows, int total,
                                                   long snapshotTimeMillis) {
        List<Item> items = new ArrayList<>(rows.size());
        for (TailCacheFileMemoryStats row : rows) {
            items.add(new Item(row));
        }
        return new TailCacheFileMemoryListResp(true, snapshotTimeMillis, total, Collections.unmodifiableList(items));
    }

    public static TailCacheFileMemoryListResp notInitialized(long nowMillis) {
        return new TailCacheFileMemoryListResp(false, nowMillis, 0, Collections.emptyList());
    }

    public boolean isInitialized() {
        return initialized;
    }

    public long getSnapshotTimeMillis() {
        return snapshotTimeMillis;
    }

    public int getTotal() {
        return total;
    }

    public boolean isTruncated() {
        return truncated;
    }

    public List<Item> getFiles() {
        return files;
    }

    public static class Item {
        private final String kind;
        private final String path;
        private final long cacheSizeBytes;
        private final long unDurableBytes;

        Item(TailCacheFileMemoryStats row) {
            this.kind = row.getKind().name();
            this.path = row.getPath();
            this.cacheSizeBytes = row.getCacheSizeBytes();
            this.unDurableBytes = row.getUnDurableBytes();
        }

        public String getKind() {
            return kind;
        }

        public String getPath() {
            return path;
        }

        public long getCacheSizeBytes() {
            return cacheSizeBytes;
        }

        public long getUnDurableBytes() {
            return unDurableBytes;
        }
    }
}
