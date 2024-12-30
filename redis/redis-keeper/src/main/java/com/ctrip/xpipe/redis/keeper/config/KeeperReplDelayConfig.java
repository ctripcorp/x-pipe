package com.ctrip.xpipe.redis.keeper.config;

public class KeeperReplDelayConfig {

    private String srcDc;

    private String destDc;

    private long delayMilli;

    public String getSrcDc() {
        return srcDc;
    }

    public void setSrcDc(String srcDc) {
        this.srcDc = srcDc;
    }

    public String getDestDc() {
        return destDc;
    }

    public void setDestDc(String destDc) {
        this.destDc = destDc;
    }

    public long getDelayMilli() {
        return delayMilli;
    }

    public void setDelayMilli(long delayMilli) {
        this.delayMilli = delayMilli;
    }

}
