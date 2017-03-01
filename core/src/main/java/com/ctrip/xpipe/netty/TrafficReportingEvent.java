package com.ctrip.xpipe.netty;

/**
 * @author leoliang
 *
 *         2017年3月1日
 */
public class TrafficReportingEvent {

    private long readBytes;

    private long writtenBytes;

    private String remoteAddr;

    public TrafficReportingEvent(long readBytes, long writtenBytes, String remoteAddr) {
        this.readBytes = readBytes;
        this.writtenBytes = writtenBytes;
        this.remoteAddr = remoteAddr;
    }

    public long getReadBytes() {
        return readBytes;
    }

    public long getWrittenBytes() {
        return writtenBytes;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

}
