package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/**
 * @author lishanglin
 * date 2022/3/2
 */
public class CommandFileSegment {

    @NonNull
    private CommandFileOffsetGtidIndex startIdx;

    @Nullable
    private CommandFileOffsetGtidIndex endIdx;

    public CommandFileSegment(CommandFileOffsetGtidIndex startIdx) {
        this(startIdx, null);
    }

    public CommandFileSegment(CommandFileOffsetGtidIndex startIdx, CommandFileOffsetGtidIndex endIdx) {
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public boolean rightBoundOpen() {
        return null == endIdx;
    }

    public GtidSet gtidSetIncluded() {
        if (null == endIdx) throw new IllegalArgumentException("segment right bound open");
        return endIdx.getExcludedGtidSet().subtract(startIdx.getExcludedGtidSet());
    }

    public CommandFileOffsetGtidIndex getStartIdx() {
        return startIdx;
    }

    public CommandFileOffsetGtidIndex getEndIdx() {
        return endIdx;
    }

    @Override
    public String toString() {
        return String.format("Segment{%s:%s}", startIdx, null == endIdx ? "-" : endIdx);
    }
}
