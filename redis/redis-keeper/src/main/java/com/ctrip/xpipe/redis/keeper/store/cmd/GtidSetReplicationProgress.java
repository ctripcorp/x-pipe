package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;

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
}
