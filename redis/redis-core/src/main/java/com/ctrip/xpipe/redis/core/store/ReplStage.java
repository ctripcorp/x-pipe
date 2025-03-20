package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.gtid.GtidSet;

public class ReplStage {

    public enum ReplProto {
        PSYNC,
        XSYNC
    }

    // common
    ReplProto proto;

    String replId;

    long begOffsetBacklog;

    long begOffsetRepl;

    // for PSYNC proto
    String replId2;

    long secondReplOffset;

    // for XSYNC proto
    String masterUuid;

    GtidSet beginGtidset;

    public boolean shiftReplId(String replId, long replOffset) {
        if (this.proto != ReplProto.PSYNC) {
            return false;
        }

        this.replId2 = this.replId;
        this.secondReplOffset = replOffset;
        this.replId = replId;
        return true;
    }

    public boolean shiftMasterUuid(String masterUuid) {
        if (this.proto != ReplProto.XSYNC) {
            return false;
        }

        this.masterUuid = masterUuid;
        return true;
    }

    public ReplStage(String replId, long replOffset, long backlogOffset) {
        this.proto = ReplProto.PSYNC;
        this.replId = replId;
        this.begOffsetRepl = replOffset;
        this.begOffsetBacklog = backlogOffset;
    }

    public ReplStage(GtidSet gtidSet, String masterUuid, long backlogOffset){
        this.proto = ReplProto.XSYNC;
        this.beginGtidset = gtidSet;
        this.masterUuid = masterUuid;
        this.begOffsetBacklog = backlogOffset;
    }

    public long replOffset2BacklogOffset(long replOffset) {
        if (this.proto != ReplProto.PSYNC || replOffset < this.begOffsetRepl) return -1L;
        return replOffset - this.begOffsetRepl + this.begOffsetBacklog;
    }

    public long backlogOffset2ReplOffset(long backlogOffset) {
        if (this.proto != ReplProto.XSYNC || backlogOffset < this.begOffsetBacklog) return -1L;
        return backlogOffset - this.begOffsetBacklog + this.begOffsetRepl;
    }

    public ReplProto getProto() {
        return proto;
    }

    public String getReplId() {
        return replId;
    }

    public String getMasterUuid() {
        return masterUuid;
    }

    public String getReplId2() {
        return replId2;
    }

    public long getSecondReplOffset() {
        return secondReplOffset;
    }

    public GtidSet getBeginGtidset() {
        return beginGtidset;
    }

    public long getBegOffsetBacklog() {
        return begOffsetBacklog;
    }

    public long getBegOffsetRepl() {
        return begOffsetRepl;
    }
}
