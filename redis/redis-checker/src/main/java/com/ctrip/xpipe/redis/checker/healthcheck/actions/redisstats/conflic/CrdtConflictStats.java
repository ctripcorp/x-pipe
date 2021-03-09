package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;

public class CrdtConflictStats {

    private Long typeConflict;

    private Long nonTypeConflict;

    private Long setConflict;

    private Long delConflict;

    private Long setDelConflict;

    private Long modifyConflict;

    private Long mergeConflict;

    private static final String KEY_CONFLICT = "crdt_conflict";
    private static final String KEY_CONFLICT_OP = "crdt_conflict_op";
    private static final String COMMA_SPLITTER = "\\s*,\\s*";
    private static final String EQUAL_SPLITTER = "\\s*=\\s*";

    private static final String KEY_TYPE_CONFLICT = "type";
    private static final String KEY_SET_CONFLICT = "set";
    private static final String KEY_DEL_CONFLICT = "del";
    private static final String KEY_SET_DEL_CONFLICT = "set_del";
    private static final String KEY_MODIFY_OP = "modify";
    private static final String KEY_MERGE_OP = "merge";

    private static final String OLD_KEY_TYPE_CONFLICT = "crdt_type_conflict";
    private static final String OLD_KEY_NON_TYPE_CONFLICT = "crdt_non_type_conflict";
    private static final String OLD_KEY_MODIFY_CONFLICT = "crdt_modify_conflict";
    private static final String OLD_KEY_MERGE_CONFLICT = "crdt_merge_conflict";

    public CrdtConflictStats(long type, long nonType, long modify, long merge) {
        typeConflict = type;
        nonTypeConflict = nonType;
        modifyConflict = modify;
        mergeConflict = merge;
    }

    public CrdtConflictStats(long type, long set, long del, long setDel, long modify, long merge) {
        typeConflict = type;
        setConflict = set;
        delConflict = del;
        setDelConflict = setDel;
        nonTypeConflict = set + del + setDel;
        modifyConflict = modify;
        mergeConflict = merge;
    }

    public CrdtConflictStats(InfoResultExtractor extractor) {
        String rawConflict = extractor.extract(KEY_CONFLICT);
        String rawConflictOp = extractor.extract(KEY_CONFLICT_OP);

        if (!StringUtil.isEmpty(rawConflict) && !StringUtil.isEmpty(rawConflictOp)) {
            initWithRawConflict(rawConflict);
            initWithRawConflictOp(rawConflictOp);
        } else {
            // compatible with low version xredis
            typeConflict = extractor.extractAsLong(OLD_KEY_TYPE_CONFLICT);
            nonTypeConflict = extractor.extractAsLong(OLD_KEY_NON_TYPE_CONFLICT);
            modifyConflict = extractor.extractAsLong(OLD_KEY_MODIFY_CONFLICT);
            mergeConflict = extractor.extractAsLong(OLD_KEY_MERGE_CONFLICT);
            setConflict = null;
            delConflict = null;
            setDelConflict = null;
        }
    }

    private void initWithRawConflict(String rawConflict) {
        String[] conflicts = StringUtil.splitRemoveEmpty(COMMA_SPLITTER, rawConflict);
        for (String conflict: conflicts) {
            String[] keyVal = conflict.split(EQUAL_SPLITTER);
            if (keyVal.length < 2) continue;

            switch (keyVal[0]) {
                case KEY_TYPE_CONFLICT:
                    typeConflict = parseLong(keyVal[1]);
                    break;
                case KEY_SET_CONFLICT:
                    setConflict = parseLong(keyVal[1]);
                    break;
                case KEY_DEL_CONFLICT:
                    delConflict = parseLong(keyVal[1]);
                    break;
                case KEY_SET_DEL_CONFLICT:
                    setDelConflict = parseLong(keyVal[1]);
                default:
            }
        }

        nonTypeConflict = setConflict + delConflict + setDelConflict;
    }

    private void initWithRawConflictOp(String rawConflictOp) {
        String[] opList = StringUtil.splitRemoveEmpty(COMMA_SPLITTER, rawConflictOp);
        for (String op: opList) {
            String[] keyVal = op.split(EQUAL_SPLITTER);
            if (keyVal.length < 2) continue;

            switch (keyVal[0]) {
                case KEY_MODIFY_OP:
                    modifyConflict = parseLong(keyVal[1]);
                    break;
                case KEY_MERGE_OP:
                    mergeConflict = parseLong(keyVal[1]);
                    break;
                default:
            }
        }
    }

    public Long getTypeConflict() {
        return typeConflict;
    }

    public Long getNonTypeConflict() {
        return nonTypeConflict;
    }

    public Long getModifyConflict() {
        return modifyConflict;
    }

    public Long getMergeConflict() {
        return mergeConflict;
    }

    public Long getSetConflict() {
        return setConflict;
    }

    public Long getDelConflict() {
        return delConflict;
    }

    public Long getSetDelConflict() {
        return setDelConflict;
    }

    public Long parseLong(String val) {
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return null;
        }
    }

}
