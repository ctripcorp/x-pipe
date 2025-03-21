package com.ctrip.xpipe.redis.keeper.store.gtid.index;

public interface StreamCommandListener {
    void onCommand(String gtid, long offset);
}
