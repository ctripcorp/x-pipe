package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.utils.OffsetNotifier;

public interface OffsetNotifyingCommandWriter {

    void setOffsetNotifier(OffsetNotifier offsetNotifier);
}
