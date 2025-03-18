package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.gtid.GtidSet;

public class ReplStage {

    public enum ReplProto {
        PSYNC,
        XSYNC
    }

    ReplProto proto;

    // for PSYNC proto
    String replId;

    long beginOffset;

    // for XSYNC proto
    GtidSet beginGtidset;

    // offset of cmd files
    long backlogOffset;

    public ReplStage(String replId, long beginOffset, long backlogOffset) {
        this.proto = ReplProto.PSYNC;
        this.replId = replId;
        this.beginOffset = beginOffset;
        this.backlogOffset = backlogOffset;
    }

    public ReplStage(GtidSet gtidSet, long backlogOffset){
        this.proto = ReplProto.XSYNC;
        this.beginGtidset = gtidSet;
        this.backlogOffset = backlogOffset;
    }

}
