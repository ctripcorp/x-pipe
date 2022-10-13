package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

/**
 * @author lishanglin
 * date 2022/6/23
 */
public class StreamNack {

    private StreamID streamId;

    private long deliveryMillSecond;

    private long deliveryCnt;

    public StreamNack(StreamID streamID, long deliveryMillSecond, long deliveryCnt) {
        this.streamId = streamID;
        this.deliveryMillSecond = deliveryMillSecond;
        this.deliveryCnt = deliveryCnt;
    }

    public StreamID getStreamId() {
        return streamId;
    }

    public byte[] getStreamIdBytes() {
        return streamId.toString().getBytes();
    }

    public long getDeliveryMillSecond() {
        return deliveryMillSecond;
    }

    public byte[] getDeliveryMillSecondBytes() {
        return String.valueOf(deliveryMillSecond).getBytes();
    }

    public long getDeliveryCnt() {
        return deliveryCnt;
    }

    public byte[] getDeliveryCntBytes() {
        return String.valueOf(deliveryCnt).getBytes();
    }

}
