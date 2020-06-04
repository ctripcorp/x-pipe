package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.utils.StringUtil;

public enum ClusterType {
    ONE_WAY(true, true),
    BI_DIRECTION(false, false);

    private boolean supportKeeper;
    private boolean supportMigration;

    ClusterType(boolean supportKeeper, boolean supportMigration) {
        this.supportKeeper = supportKeeper;
        this.supportMigration = supportMigration;
    }

    public boolean supportKeeper() {
        return this.supportKeeper;
    }

    public boolean supportMigration() {
        return this.supportMigration;
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
