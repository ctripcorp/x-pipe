package com.ctrip.xpipe.redis.core.entity;

/**
 * @author lishanglin
 * date 2023/11/9
 */
public class KeeperDiskInfo {

    public long timestamp;

    public String keeperContainerIp;

    public boolean available;

    public DiskSpaceUsageInfo spaceUsageInfo;

    public DiskIOStatInfo ioStatInfo;

    public KeeperDiskInfo() {
        this.timestamp = System.currentTimeMillis()/1000;
        this.keeperContainerIp = null;
        this.available = false;
        this.spaceUsageInfo = null;
        this.ioStatInfo = null;
    }

    @Override
    public String toString() {
        return "KeeperDiskInfo{" +
                "timestamp=" + timestamp +
                ", keeperContainerIp='" + keeperContainerIp + '\'' +
                ", available=" + available +
                ", spaceUsageInfo=" + spaceUsageInfo +
                ", ioStatInfo=" + ioStatInfo +
                '}';
    }
}
