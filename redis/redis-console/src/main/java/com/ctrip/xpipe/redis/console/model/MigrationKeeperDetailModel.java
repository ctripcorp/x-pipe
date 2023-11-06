package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.checker.model.DcClusterShard;

public class MigrationKeeperDetailModel {

    private DcClusterShard migrateShad;

    private String targetKeeperContainerIp;

    public DcClusterShard getMigrateShad() {
        return migrateShad;
    }

    public MigrationKeeperDetailModel setMigrateShad(DcClusterShard migrateShad) {
        this.migrateShad = migrateShad;
        return this;
    }

    public String getTargetKeeperContainerIp() {
        return targetKeeperContainerIp;
    }

    public MigrationKeeperDetailModel setTargetKeeperContainerIp(String targetKeeperContainerIp) {
        this.targetKeeperContainerIp = targetKeeperContainerIp;
        return this;
    }

    @Override
    public String toString() {
        return "MigrationKeeperDetailModel{" +
                "migrateShad=" + migrateShad +
                ", targetKeeperContainerIp='" + targetKeeperContainerIp + '\'' +
                '}';
    }
}
