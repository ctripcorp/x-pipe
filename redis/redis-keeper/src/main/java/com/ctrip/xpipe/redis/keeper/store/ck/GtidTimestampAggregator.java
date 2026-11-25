package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidTimestampItem;
import com.ctrip.xpipe.gtid.GtidSet;

import java.util.*;

/**
 * Aggregates cumulative GTID watermarks into minute buckets.
 *
 * update, seed and drain are synchronized because updates run on the CK consumer
 * while drains run on the scheduled reporter thread.
 */
class GtidTimestampAggregator {

    static final long SECONDS_PER_MINUTE = 60L;

    private final long shardId;

    private final Map<String, Long> watermarks = new HashMap<>();

    private final TreeMap<Long, Map<String, Long>> minuteBuckets = new TreeMap<>();

    GtidTimestampAggregator(long shardId) {
        this.shardId = shardId;
    }

    synchronized void seed(GtidSet executedGtidSet) {
        if (executedGtidSet == null) {
            return;
        }

        for (GtidSet.UUIDSet uuidSet : executedGtidSet.getUUIDSets()) {
            if (uuidSet == null || uuidSet.isEmpty()) {
                continue;
            }
            watermarks.merge(uuidSet.getUUID(), uuidSet.getLastGno(), Math::max);
        }
    }

    synchronized void update(String uuid, long seq, long timestamp) {
        if (uuid == null || uuid.isEmpty()) {
            return;
        }

        long watermark = Math.max(watermarks.getOrDefault(uuid, 0L), seq);
        watermarks.put(uuid, watermark);

        long minute = alignToMinute(timestamp);
        minuteBuckets.computeIfAbsent(minute, ignored -> new HashMap<>())
                .merge(uuid, watermark, Math::max);
    }

    synchronized List<GtidTimestampItem> drainExpired(long timestamp) {
        long currentMinute = alignToMinute(timestamp);
        List<GtidTimestampItem> result = new ArrayList<>();
        Iterator<Map.Entry<Long, Map<String, Long>>> iterator = minuteBuckets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Map<String, Long>> entry = iterator.next();
            if (entry.getKey() >= currentMinute) {
                break;
            }
            appendItems(result, entry.getKey(), entry.getValue());
            iterator.remove();
        }
        return result;
    }

    synchronized List<GtidTimestampItem> drainAll() {
        List<GtidTimestampItem> result = new ArrayList<>();
        for (Map.Entry<Long, Map<String, Long>> entry : minuteBuckets.entrySet()) {
            appendItems(result, entry.getKey(), entry.getValue());
        }
        minuteBuckets.clear();
        return result;
    }

    synchronized int bucketCount() {
        return minuteBuckets.size();
    }

    synchronized Long watermark(String uuid) {
        return watermarks.get(uuid);
    }

    private void appendItems(List<GtidTimestampItem> target, long minute, Map<String, Long> uuidWatermarks) {
        for (Map.Entry<String, Long> entry : uuidWatermarks.entrySet()) {
            target.add(GtidTimestampItem.build(minute, shardId, entry.getKey(), entry.getValue()));
        }
    }

    static long alignToMinute(long timestamp) {
        return timestamp / SECONDS_PER_MINUTE * SECONDS_PER_MINUTE;
    }
}
