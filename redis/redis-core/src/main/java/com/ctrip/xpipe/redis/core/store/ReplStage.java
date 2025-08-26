package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;
import java.util.Objects;

public class ReplStage {

    public enum ReplProto {
        PSYNC,
        XSYNC;

        public String asString() {
            return this.name();
        }

        public static ReplProto fromString(String name) {
            if (name == null) {
                return null;
            }
            try {
                return ReplProto.valueOf(name.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
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
        if (!this.replId.equals(replId)) {
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

    public boolean adjustBegOffsetRepl(long begOffsetRepl, long begOffsetBacklog) {
        // begOffsetRepl - newBegOffsetRepl == begOffsetBacklog - this.begOffsetBacklog
        long newBegOffsetRepl = begOffsetRepl - begOffsetBacklog + this.begOffsetBacklog;
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
        return this.gtidLost == null ? new GtidSet(GtidSet.EMPTY_GTIDSET) : this.gtidLost;
    }

    public void setGtidLost(GtidSet gtidLost) {
        this.gtidLost = gtidLost == null ? new GtidSet(GtidSet.EMPTY_GTIDSET) : gtidLost.clone();
    }

    public ReplStage(String replId, long beginReplOffset, long begOffsetBacklog) {
        this.proto = ReplProto.PSYNC;
        this.replId = replId;
        this.begOffsetRepl = beginReplOffset;
        this.begOffsetBacklog = begOffsetBacklog;
        this.replId2 = ReplicationStoreMeta.EMPTY_REPL_ID;
        this.secondReplIdOffset = ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET;
    }

    public ReplStage(String replId, long begOffsetRepl, long begOffsetBacklog,
                     String masterUuid, GtidSet gtidLost, GtidSet gtidBegin) {
        this.proto = ReplProto.XSYNC;
        this.replId = replId;
        this.begOffsetRepl = begOffsetRepl;
        this.begOffsetBacklog = begOffsetBacklog;
        this.masterUuid = masterUuid;
        this.beginGtidset = gtidBegin == null ? new GtidSet(GtidSet.EMPTY_GTIDSET) : gtidBegin.clone();
        this.gtidLost = gtidLost == null ? new GtidSet(GtidSet.EMPTY_GTIDSET) : gtidLost.clone();
        this.replId2 = null;
        this.secondReplIdOffset = ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET;
    }

    public ReplStage(ReplStage original) {
        this.proto = original.proto;
        this.replId = original.replId;
        this.begOffsetBacklog = original.begOffsetBacklog;
        this.begOffsetRepl = original.begOffsetRepl;
        this.replId2 = original.replId2;
        this.secondReplIdOffset = original.secondReplIdOffset;
        this.masterUuid = original.masterUuid;
        this.beginGtidset = original.beginGtidset == null ? new GtidSet(GtidSet.EMPTY_GTIDSET) : original.beginGtidset.clone();
        this.gtidLost = original.gtidLost == null ? new GtidSet(GtidSet.EMPTY_GTIDSET) : original.gtidLost.clone();
    }

    @JsonCreator
    public ReplStage(
            @JsonProperty("proto") String proto,
            @JsonProperty("replId") String replId,
            @JsonProperty("begOffsetRepl") long begOffsetRepl,
            @JsonProperty("begOffsetBacklog") long begOffsetBacklog,
            @JsonProperty("replId2") String replId2,
            @JsonProperty("secondReplIdOffset") long secondReplIdOffset,
            @JsonProperty("masterUuid") String masterUuid,
            @JsonProperty("gtidLost") String gtidLost,
            @JsonProperty("beginGtidset") String beginGtidset) {

        this.proto = ReplProto.fromString(proto);
        this.replId = replId;
        this.begOffsetRepl = begOffsetRepl;
        this.begOffsetBacklog = begOffsetBacklog;
        this.masterUuid = masterUuid;
        this.beginGtidset = (beginGtidset == null || beginGtidset.isEmpty()) ? new GtidSet(GtidSet.EMPTY_GTIDSET) : new GtidSet(beginGtidset);
        this.gtidLost = (gtidLost == null || gtidLost.isEmpty()) ? new GtidSet(GtidSet.EMPTY_GTIDSET) : new GtidSet(gtidLost);
        this.replId2 =  replId2;
        this.secondReplIdOffset = secondReplIdOffset;
    }

    public long replOffset2BacklogOffset(long replOffset) {
        if (replOffset < this.begOffsetRepl) {
            throw new IllegalArgumentException("unexpected replOffset " + replOffset + " while begin at " + this.begOffsetRepl);
        }
        return replOffset - this.begOffsetRepl + this.begOffsetBacklog;
    }

    public long backlogOffset2ReplOffset(long backlogOffset) {
        if (backlogOffset < this.begOffsetBacklog) {
            throw new IllegalArgumentException("unexpected backlogOffset " + backlogOffset + " while begin at " + this.begOffsetBacklog);
        }
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
        return beginGtidset == null ? new GtidSet(GtidSet.EMPTY_GTIDSET) : beginGtidset;
    }

    public long getBegOffsetBacklog() {
        return begOffsetBacklog;
    }

    public long getBegOffsetRepl() {
        return begOffsetRepl;
    }

    @Override
    public String toString() {
        if (proto.equals(ReplProto.XSYNC)) {
            return String.format("XSYNC|%s;%d:%d@%s", beginGtidset, begOffsetRepl, begOffsetBacklog, masterUuid);
        } else {
            return String.format("PSYNC|%s:%d:%d;%s:%d", replId, begOffsetRepl, begOffsetBacklog, replId2, secondReplIdOffset);
        }
    }

    private boolean gtidSetEquals(GtidSet a, GtidSet b) {
        boolean aIsEmpty = a == null || a.isEmpty();
        boolean bIsEmpty = b == null || b.isEmpty();
        return (aIsEmpty && bIsEmpty) || (!aIsEmpty && !bIsEmpty && a.equals(b));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplStage replStage = (ReplStage) o;
        return begOffsetBacklog == replStage.begOffsetBacklog &&
                begOffsetRepl == replStage.begOffsetRepl &&
                secondReplIdOffset == replStage.secondReplIdOffset &&
                proto == replStage.proto &&
                replId.equals(replStage.replId) &&
                Objects.equals(replId2, replStage.replId2) &&
                Objects.equals(masterUuid, replStage.masterUuid) &&
                gtidSetEquals(beginGtidset, replStage.beginGtidset) &&
                gtidSetEquals(gtidLost, replStage.gtidLost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(proto, replId, begOffsetBacklog, begOffsetRepl, replId2, secondReplIdOffset, masterUuid, beginGtidset, gtidLost);
    }
}
