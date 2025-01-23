package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.utils.StringUtil;

public enum ClusterType {
    ONE_WAY(true, true, true, false, false, true),
    BI_DIRECTION(false, false, true, true),
    // TODO: single_dc and local_dc support health check
    //
    // Currently, sentinel health check is on for single_dc & local_dc via config/console.sentinel.check.outer.clusters
    //     sentinel health check is off by default for single_dc & local_dc.
    //
    // Finally, config/console.sentinel.check.outer.clusters would be removed 
    //     and both sentinel & redis health would be controlled by supportHealthCheck flag.
    SINGLE_DC(false, false, false, false),
    LOCAL_DC(false, false, false, true),
    CROSS_DC(false, false, false, true,true),
    // 异构集群，具体集群类型由AzGroup中的clusterType来表示
    HETERO(true, true, true, false);

    private boolean supportKeeper;
    private boolean supportMigration;
    private boolean supportHealthCheck;
    private boolean supportMultiActiveDC;
    private boolean isCrossDc;
    private boolean supportApplier;

    ClusterType(boolean supportKeeper, boolean supportMigration, boolean supportHealthCheck, boolean supportMultiActiveDC) {
        this(supportKeeper, supportMigration, supportHealthCheck, supportMultiActiveDC, false, false);
    }

    ClusterType(boolean supportKeeper, boolean supportMigration, boolean supportHealthCheck, boolean supportMultiActiveDC, boolean isCrossDc) {
        this(supportKeeper, supportMigration, supportHealthCheck, supportMultiActiveDC, isCrossDc, false);
    }

    ClusterType(boolean supportKeeper, boolean supportMigration, boolean supportHealthCheck, boolean supportMultiActiveDC, boolean isCrossDc, boolean supportApplier) {
        this.supportKeeper = supportKeeper;
        this.supportMigration = supportMigration;
        this.supportHealthCheck = supportHealthCheck;
        this.supportMultiActiveDC = supportMultiActiveDC;
        this.isCrossDc = isCrossDc;
        this.supportApplier = supportApplier;
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

    public boolean supportApplier() {
        return this.supportApplier;
    }

    public boolean isCrossDc() {
        return isCrossDc;
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

    public static boolean supportMultiGroup(String type) {
        if (StringUtil.isEmpty(type)) {
            return false;
        }
        return isSameClusterType(type, ONE_WAY) || isSameClusterType(type, BI_DIRECTION)
            || isSameClusterType(type, HETERO);
    }

    public static boolean supportUpgradeAzGroup(String type) {
        if (StringUtil.isEmpty(type)) {
            return false;
        }
        return isSameClusterType(type, ONE_WAY) || isSameClusterType(type, BI_DIRECTION)
            || isSameClusterType(type, SINGLE_DC);
    }

    public static boolean supportConvert(String type) {
        if (StringUtil.isEmpty(type)) {
            return false;
        }
        return isSameClusterType(type, ONE_WAY) || isSameClusterType(type, HETERO);
    }

    public static boolean supportConvert(String oldType, String newType) {
        if(StringUtil.isEmpty(oldType) || StringUtil.isEmpty(newType)) {
            return false;
        }
        if(isSameClusterType(oldType, HETERO) &&
                (isSameClusterType(newType, ONE_WAY) || isSameClusterType(newType, SINGLE_DC))) {
            return true;
        }
        if((isSameClusterType(oldType, ONE_WAY) || isSameClusterType(oldType, SINGLE_DC)) && isSameClusterType(newType, HETERO)) {
            return true;
        }
        return false;
    }

    public static ClusterType lookup(String name) {
        if (StringUtil.isEmpty(name)) throw new IllegalArgumentException("no ClusterType for name " + name);
        return valueOf(name.toUpperCase());
    }

}
