package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/4/18
 */
public class GtidSetReplicationProgress implements ReplicationProgress<GtidSet, String> {

    private GtidSet gtidSet;

    public GtidSetReplicationProgress(GtidSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    @Override
    public GtidSet getProgress() {
        return gtidSet;
    }

    @Override
    public void makeProgress(String gtid) {
        gtidSet.add(gtid);
    }

    @Override
    public TYPE getType() {
        return TYPE.GTIDSET;
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
