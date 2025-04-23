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

    long secondReplIdOffset;

    // for XSYNC proto
    String masterUuid;

    GtidSet beginGtidset;
    GtidSet gtidLost;

    public void setReplId2(String replId2) {
        this.replId2 = replId2;
    }

    public void setSecondReplIdOffset(long secondReplIdOffset) {
        this.secondReplIdOffset = secondReplIdOffset;
    }

    public boolean updateReplId(String replId) {
        if (this.replId != replId) {
            this.replId = replId;
            return true;
        } else {
            return false;
        }
    }

    public boolean updateBegOffsetRepl(long begOffsetRepl) {
        if (this.begOffsetRepl != begOffsetRepl) {
            this.begOffsetRepl = begOffsetRepl;
            return true;
        } else {
            return false;
        }
    }

    public boolean adjustBegOffsetRepl(long replOff, long backlogOff) {
        // reploff - newBegOffsetRepl == backlogOff - begOffsetBacklog
        long newBegOffsetRepl = replOff - backlogOff + begOffsetBacklog;
        if (this.begOffsetRepl != newBegOffsetRepl) {
            this.begOffsetRepl = newBegOffsetRepl;
            return true;
        } else {
            return false;
        }
    }

    public boolean updateMasterUuid(String masterUuid) {
        if (!this.masterUuid.equals(masterUuid)) {
            this.masterUuid = masterUuid;
            return true;
        } else {
            return false;
        }
    }

    public GtidSet getGtidLost() {
        return this.gtidLost;
    }

    public void setGtidLost(GtidSet gtidLost) {
        this.gtidLost = gtidLost;
    }

    public ReplStage(String replId, long beginReplOffset, long backlogOffset) {
        this.proto = ReplProto.PSYNC;
        this.replId = replId;
        this.begOffsetRepl = beginReplOffset;
        this.begOffsetBacklog = backlogOffset;
        this.replId2 = ReplicationStoreMeta.EMPTY_REPL_ID;
        this.secondReplIdOffset = ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET;
    }

    public ReplStage(String replId, long beginOffsetRepl, long backlogOffset,
                     String masterUuid, GtidSet gtidLost, GtidSet gtidExecuted) {
        this.proto = ReplProto.XSYNC;
        this.replId = replId;
        this.begOffsetRepl = beginOffsetRepl;
        this.begOffsetBacklog = backlogOffset;
        this.masterUuid = masterUuid;
        this.beginGtidset = gtidExecuted;
        this.gtidLost = gtidLost;
        this.replId2 = null;
        this.secondReplIdOffset = ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET;
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

    public long getSecondReplIdOffset() {
        return secondReplIdOffset;
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
