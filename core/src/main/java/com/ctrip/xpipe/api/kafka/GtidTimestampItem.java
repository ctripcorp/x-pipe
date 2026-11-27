package com.ctrip.xpipe.api.kafka;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Kafka item used to persist a GTID end-sequence watermark for one minute.
 */
@org.apache.avro.specific.AvroGenerated
public class GtidTimestampItem extends SpecificRecordBase implements SpecificRecord {

    private static final long serialVersionUID = 1L;

    public static final Schema SCHEMA$ = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"GtidTimestampItem\","
                    + "\"namespace\":\"com.ctrip.xpipe.api.kafka\",\"fields\":["
                    + "{\"name\":\"timestamp\",\"type\":\"long\"},"
                    + "{\"name\":\"shardid\",\"type\":\"int\"},"
                    + "{\"name\":\"uuid\",\"type\":\"string\"},"
                    + "{\"name\":\"end_seq\",\"type\":\"long\"}]}");

    private static final SpecificData MODEL$ = new SpecificData();

    private long timestamp;
    private int shardid;
    private CharSequence uuid;
    private long endSeq;

    public GtidTimestampItem() {
    }

    public GtidTimestampItem(long timestamp, int shardid, CharSequence uuid, long endSeq) {
        this.timestamp = timestamp;
        this.shardid = shardid;
        this.uuid = uuid;
        this.endSeq = endSeq;
    }

    public static Schema getClassSchema() {
        return SCHEMA$;
    }

    @Override
    public SpecificData getSpecificData() {
        return MODEL$;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0:
                return timestamp;
            case 1:
                return shardid;
            case 2:
                return uuid;
            case 3:
                return endSeq;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field);
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0:
                timestamp = (Long) value;
                break;
            case 1:
                shardid = (Integer) value;
                break;
            case 2:
                uuid = (CharSequence) value;
                break;
            case 3:
                endSeq = (Long) value;
                break;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field);
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getShardid() {
        return shardid;
    }

    public void setShardid(int shardid) {
        this.shardid = shardid;
    }

    public CharSequence getUuid() {
        return uuid;
    }

    public void setUuid(CharSequence uuid) {
        this.uuid = uuid;
    }

    public long getEndSeq() {
        return endSeq;
    }

    public void setEndSeq(long endSeq) {
        this.endSeq = endSeq;
    }

    public static GtidTimestampItem build(long timestamp, long shardid, String uuid, long endSeq) {
        return new GtidTimestampItem(timestamp, (int) shardid, uuid, endSeq);
    }
}
