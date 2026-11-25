package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidTimestampItem;
import com.ctrip.xpipe.gtid.GtidSet;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

public class GtidTimestampAggregatorTest {

    private static final String UUID_A = "000000000000000000000000000000000000000A";
    private static final String UUID_B = "000000000000000000000000000000000000000B";

    @Test
    public void shouldAggregateMaximumWatermarkForMinute() {
        GtidTimestampAggregator aggregator = new GtidTimestampAggregator(7L);

        aggregator.update(UUID_A, 10L, 61L);
        aggregator.update(UUID_A, 8L, 62L);
        aggregator.update(UUID_A, 12L, 119L);
        aggregator.update(UUID_B, 5L, 90L);

        List<GtidTimestampItem> items = aggregator.drainExpired(120L);

        Assert.assertEquals(2, items.size());
        assertItem(items, UUID_A, 60L, 12L, 7);
        assertItem(items, UUID_B, 60L, 5L, 7);
        Assert.assertEquals(0, aggregator.bucketCount());
    }

    @Test
    public void shouldSeedCumulativeWatermarkBeforeBacklogReplay() {
        GtidTimestampAggregator aggregator = new GtidTimestampAggregator(1L);
        aggregator.seed(new GtidSet(UUID_A + ":1-10000"));

        aggregator.update(UUID_A, 9600L, 300L);

        List<GtidTimestampItem> items = aggregator.drainExpired(360L);
        Assert.assertEquals(1, items.size());
        assertItem(items, UUID_A, 300L, 10000L, 1);
        Assert.assertEquals(Long.valueOf(10000L), aggregator.watermark(UUID_A));
    }

    @Test
    public void shouldDrainLateExpiredBucketOnNextRun() {
        GtidTimestampAggregator aggregator = new GtidTimestampAggregator(1L);

        Assert.assertTrue(aggregator.drainExpired(180L).isEmpty());

        // An event stamped in an older minute can be consumed after that minute was already checked.
        aggregator.update(UUID_A, 10L, 119L);

        List<GtidTimestampItem> items = aggregator.drainExpired(240L);
        Assert.assertEquals(1, items.size());
        assertItem(items, UUID_A, 60L, 10L, 1);
    }

    @Test
    public void shouldKeepCurrentMinuteUntilItCloses() {
        GtidTimestampAggregator aggregator = new GtidTimestampAggregator(1L);
        aggregator.update(UUID_A, 10L, 121L);

        Assert.assertTrue(aggregator.drainExpired(179L).isEmpty());
        Assert.assertEquals(1, aggregator.bucketCount());
        Assert.assertEquals(1, aggregator.drainExpired(180L).size());
    }

    @Test
    public void shouldSerializeTimestampItemAsSpecificRecord() throws Exception {
        GtidTimestampItem expected = GtidTimestampItem.build(120L, 7L, UUID_A, 99L);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SpecificDatumWriter<GtidTimestampItem> writer = new SpecificDatumWriter<>(GtidTimestampItem.class);
        org.apache.avro.io.BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        writer.write(expected, encoder);
        encoder.flush();

        SpecificDatumReader<GtidTimestampItem> reader = new SpecificDatumReader<>(GtidTimestampItem.class);
        GtidTimestampItem actual = reader.read(null,
                DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(output.toByteArray()), null));

        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        Assert.assertEquals(expected.getShardid(), actual.getShardid());
        Assert.assertEquals(expected.getUuid().toString(), actual.getUuid().toString());
        Assert.assertEquals(expected.getEndSeq(), actual.getEndSeq());
    }

    private void assertItem(List<GtidTimestampItem> items, String uuid, long timestamp, long endSeq, int shardId) {
        GtidTimestampItem item = items.stream()
                .filter(candidate -> uuid.contentEquals(candidate.getUuid()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing item for " + uuid));
        Assert.assertEquals(timestamp, item.getTimestamp());
        Assert.assertEquals(endSeq, item.getEndSeq());
        Assert.assertEquals(shardId, item.getShardid());
    }
}
