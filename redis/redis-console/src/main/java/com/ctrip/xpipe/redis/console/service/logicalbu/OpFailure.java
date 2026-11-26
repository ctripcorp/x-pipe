package com.ctrip.xpipe.redis.console.service.logicalbu;

/**
 * One failure: which layer, which step, what code, against which remote target, affecting which scope.
 */
public class OpFailure {

    private static final int MAX_DETAIL_LENGTH = 200;

    private OpLayer layer;
    private OpStage stage;
    private OpErrorCode code;
    /** Remote side that failed: "xpipe-db", "metaserver@SHAJQ", keeper ip:port; null for pure business checks. */
    private String target;
    /** Affected scope: "dc/cluster/shard" or "dc/cluster"; null when the whole logical BU is affected. */
    private String scope;
    /** Detail for operators, e.g. root exception message or counts. */
    private String detail;

    public OpFailure() {
    }

    public OpFailure(OpLayer layer, OpStage stage, OpErrorCode code, String target, String scope, String detail) {
        this.layer = layer;
        this.stage = stage;
        this.code = code;
        this.target = target;
        this.scope = scope;
        this.detail = truncate(detail);
    }

    public static OpFailure console(OpStage stage, OpErrorCode code, String scope, String detail) {
        return new OpFailure(OpLayer.CONSOLE, stage, code, null, scope, detail);
    }

    /** Same failure re-targeted to another scope (e.g. a cluster-level notify failure fanned out to its shards). */
    public OpFailure withScope(String newScope) {
        return new OpFailure(layer, stage, code, target, newScope, detail);
    }

    private static String truncate(String s) {
        if (s == null || s.length() <= MAX_DETAIL_LENGTH) {
            return s;
        }
        return s.substring(0, MAX_DETAIL_LENGTH) + "...";
    }

    public OpLayer getLayer() {
        return layer;
    }

    public OpStage getStage() {
        return stage;
    }

    public OpErrorCode getCode() {
        return code;
    }

    public String getMessage() {
        return code == null ? null : code.getMessage();
    }

    public String getTarget() {
        return target;
    }

    public String getScope() {
        return scope;
    }

    public String getDetail() {
        return detail;
    }

    @Override
    public String toString() {
        return String.format("[%s/%s/%s] target=%s scope=%s detail=%s", layer, stage, code, target, scope, detail);
    }
}
