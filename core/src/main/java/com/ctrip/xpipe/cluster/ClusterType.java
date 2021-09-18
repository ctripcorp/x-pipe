package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.utils.StringUtil;

public enum ClusterType {
    ONE_WAY(true, true, true, false),
    BI_DIRECTION(false, false, true, true),
    // TODO: single_dc and local_dc support health check
    SINGLE_DC(false, false, false, false),
    LOCAL_DC(false, false, false, true);

    private boolean supportKeeper;
    private boolean supportMigration;
    private boolean supportHealthCheck;
    private boolean supportMultiActiveDC;

    ClusterType(boolean supportKeeper, boolean supportMigration, boolean supportHealthCheck, boolean supportMultiActiveDC) {
        this.supportKeeper = supportKeeper;
        this.supportMigration = supportMigration;
        this.supportHealthCheck = supportHealthCheck;
        this.supportMultiActiveDC = supportMultiActiveDC;
    }

    public boolean supportKeeper() {
        return this.supportKeeper;
    }

    public boolean supportMigration() {
        return this.supportMigration;
    }

    public boolean supportHealthCheck() {
        return this.supportHealthCheck;
    }

    public boolean supportMultiActiveDC() {
        return this.supportMultiActiveDC;
    }

    public boolean supportSingleActiveDC() {
        return !this.supportMultiActiveDC;
    }

    public static boolean isSameClusterType(String source, ClusterType target) {
        return source.equalsIgnoreCase(target.toString());
    }

    public static boolean isTypeValidate(String inputType) {
        if (StringUtil.isEmpty(inputType)) return false;

        for (ClusterType type: values()) {
            if (isSameClusterType(inputType, type)) return true;
        }

        return false;
    }

    public static ClusterType lookup(String name) {
        if (StringUtil.isEmpty(name)) throw new IllegalArgumentException("no ClusterType for name " + name);
        return valueOf(name.toUpperCase());
    }

}
