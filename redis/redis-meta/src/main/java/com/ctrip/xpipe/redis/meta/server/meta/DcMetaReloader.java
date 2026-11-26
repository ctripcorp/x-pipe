package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * Pull DcMeta from console and apply it through the same compare/apply path as the periodic refresh, split in two
 * steps so one instance can pull once and hand the same DcMeta to its peers (one console build per dc).
 */
public interface DcMetaReloader {

    enum ReloadStatus {
        APPLIED,
        NO_CHANGE,
        /** a clusterModified arrived after this pull started; the pulled meta may be older, so it was not applied */
        SKIPPED_RECENT_MODIFY,
        /** too many structural changes at once (META_MODIFY_PROTECT_COUNT) */
        REJECTED_TOO_MANY
    }

    final class Pulled {
        private final DcMeta dcMeta;
        /** taken right before the console call, the "data as of" time used by apply */
        private final long loadTime;

        public Pulled(DcMeta dcMeta, long loadTime) {
            this.dcMeta = dcMeta;
            this.loadTime = loadTime;
        }

        public DcMeta getDcMeta() {
            return dcMeta;
        }

        public long getLoadTime() {
            return loadTime;
        }
    }

    /** @throws Exception when console cannot be reached */
    Pulled pull() throws Exception;

    /**
     * Apply a DcMeta pulled at {@code loadTime}, by this instance or a peer. Serialized with the periodic refresh.
     * A DcMeta older than the last one this instance applied is ignored (reported as NO_CHANGE): the newer one already
     * contains everything that was in the DB at that time.
     */
    ReloadStatus apply(DcMeta future, long loadTime);
}
