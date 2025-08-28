package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.gtid.GtidSet;
import io.netty.buffer.ByteBuf;

public interface ApplierSyncObserver {

    void doOnFullSync(String replId, long replOffset);
    void doOnXFullSync(GtidSet lost, long replOffset);
    void doOnXContinue(GtidSet lost, long replOffset);
    void doOnContinue(String newReplId);
    void doOnAppendCommand(ByteBuf byteBuf);
    void endReadRdb();
    void protoChange();
}
