package com.ctrip.xpipe.redis.core.entity;

/**
 * @author lishanglin
 * date 2023/11/10
 */
public class DiskIOStatInfo {

    public String device;

    public long rrqm;

    public long wrqm;

    public long readCnt;

    public long writeCnt;

    public long readKB;

    public long writeKB;

    public long avgReqSize;

    public long avgQueueSize;

    public float await;

    public float readAwait;

    public float writeAwait;

    public float util;

    public static DiskIOStatInfo parse(String raw) {
        String[] strs = raw.split("\\s+");
        if (strs.length < 14) return null;

        DiskIOStatInfo info = new DiskIOStatInfo();
        info.device = strs[0];
        info.rrqm = Float.valueOf(strs[1]).longValue();
        info.wrqm = Float.valueOf(strs[2]).longValue();
        info.readCnt = Float.valueOf(strs[3]).longValue();
        info.writeCnt = Float.valueOf(strs[4]).longValue();
        info.readKB = Float.valueOf(strs[5]).longValue();
        info.writeKB = Float.valueOf(strs[6]).longValue();
        info.avgReqSize = Float.valueOf(strs[7]).longValue();
        info.avgQueueSize = Float.valueOf(strs[8]).longValue();
        info.await = Float.valueOf(strs[9]).longValue();
        info.readAwait = Float.valueOf(strs[10]).longValue();
        info.writeAwait = Float.valueOf(strs[11]).longValue();
        info.util = Float.valueOf(strs[12]).longValue();
        return info;
    }

    @Override
    public String toString() {
        return "DiskIOStatInfo{" +
                "device='" + device + '\'' +
                ", rrqm=" + rrqm +
                ", wrqm=" + wrqm +
                ", readCnt=" + readCnt +
                ", writeCnt=" + writeCnt +
                ", readKB=" + readKB +
                ", writeKB=" + writeKB +
                ", avgReqSize=" + avgReqSize +
                ", avgQueueSize=" + avgQueueSize +
                ", await=" + await +
                ", readAwait=" + readAwait +
                ", writeAwait=" + writeAwait +
                ", util=" + util +
                '}';
    }
}
