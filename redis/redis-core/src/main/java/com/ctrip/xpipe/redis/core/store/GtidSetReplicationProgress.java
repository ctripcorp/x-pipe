package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.gtid.GtidSet;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/4/18
 */
public class GtidSetReplicationProgress implements ReplicationProgress<GtidSet> {

    private GtidSet gtidSet;

    public GtidSetReplicationProgress(GtidSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    @Override
    public GtidSet getProgress() {
        return gtidSet;
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
