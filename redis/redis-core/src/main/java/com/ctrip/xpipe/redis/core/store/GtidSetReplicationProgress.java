package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.gtid.GtidSet;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/4/18
 */
public class GtidSetReplicationProgress implements ReplicationProgress<GtidSet> {

    private GtidSet gtidSet;

    private String progressMark;

    public GtidSetReplicationProgress(GtidSet gtidSet) {
        this(gtidSet, gtidSet.toString());
    }

    public GtidSetReplicationProgress(GtidSet gtidSet, String progressMark) {
        this.gtidSet = gtidSet;
        this.progressMark = progressMark;
    }

    @Override
    public GtidSet getProgress() {
        return gtidSet;
    }

    @Override
    public String getProgressMark() {
        return progressMark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GtidSetReplicationProgress that = (GtidSetReplicationProgress) o;
        return Objects.equals(gtidSet, that.gtidSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gtidSet);
    }

    @Override
    public String toString() {
        return String.format("gtidset[%s]", gtidSet.toString());
    }
}
