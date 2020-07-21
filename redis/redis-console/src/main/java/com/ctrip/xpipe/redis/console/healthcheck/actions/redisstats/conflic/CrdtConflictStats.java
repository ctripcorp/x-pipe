package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

public class CrdtConflictStats {

    private long typeConflict;

    private long nonTypeConflict;

    private long modifyConflict;

    private long mergeConflict;

    private static final String KEY_TYPE_CONFLICT = "crdt_type_conflict";
    private static final String KEY_NON_TYPE_CONFLICT = "crdt_non_type_conflict";
    private static final String KEY_MODIFY_CONFLICT = "crdt_modify_conflict";
    private static final String KEY_MERGE_CONFLICT = "crdt_merge_conflict";

    public CrdtConflictStats(long type, long nonType, long modify, long merge) {
        typeConflict = type;
        nonTypeConflict = nonType;
        modifyConflict = modify;
        mergeConflict = merge;
    }

    public CrdtConflictStats(InfoResultExtractor extractor) {
        typeConflict = extractor.extract(KEY_TYPE_CONFLICT, this::parseLongOrZero);
        nonTypeConflict = extractor.extract(KEY_NON_TYPE_CONFLICT, this::parseLongOrZero);
        modifyConflict = extractor.extract(KEY_MODIFY_CONFLICT, this::parseLongOrZero);
        mergeConflict = extractor.extract(KEY_MERGE_CONFLICT, this::parseLongOrZero);
    }

    public long getTypeConflict() {
        return typeConflict;
    }

    public long getNonTypeConflict() {
        return nonTypeConflict;
    }

    public long getModifyConflict() {
        return modifyConflict;
    }

    public long getMergeConflict() {
        return mergeConflict;
    }

    long parseLongOrZero(String value) {
        return null == value ? 0L : Long.parseLong(value);
    }

}
