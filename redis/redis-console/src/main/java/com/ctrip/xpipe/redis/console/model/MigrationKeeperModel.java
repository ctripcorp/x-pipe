package com.ctrip.xpipe.redis.console.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;


public class MigrationKeeperModel implements Serializable {
    private static final long serialVersionUID = 1L;

    private int maxMigrationKeeperNum;

    private KeeperContainerInfoModel srcKeeperContainer;

    private KeeperContainerInfoModel targetKeeperContainer;

    private List<ClusterTbl> migrationClusters;

    public MigrationKeeperModel() {
    }

    public int getMaxMigrationKeeperNum() {
        return maxMigrationKeeperNum;
    }

    public MigrationKeeperModel setMaxMigrationKeeperNum(int maxMigrationKeeperNum) {
        this.maxMigrationKeeperNum = maxMigrationKeeperNum;
        return this;
    }

    public KeeperContainerInfoModel getSrcKeeperContainer() {
        return srcKeeperContainer;
    }

    public MigrationKeeperModel setSrcKeeperContainer(KeeperContainerInfoModel srcKeeperContainer) {
        this.srcKeeperContainer = srcKeeperContainer;
        return this;
    }

    public KeeperContainerInfoModel getTargetKeeperContainer() {
        return targetKeeperContainer;
    }

    public MigrationKeeperModel setTargetKeeperContainer(KeeperContainerInfoModel targetKeeperContainer) {
        this.targetKeeperContainer = targetKeeperContainer;
        return this;
    }

    public List<ClusterTbl> getMigrationClusters() {
        return migrationClusters;
    }

    public MigrationKeeperModel setMigrationClusters(List<ClusterTbl> migrationClusters) {
        this.migrationClusters = migrationClusters;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationKeeperModel that = (MigrationKeeperModel) o;
        return maxMigrationKeeperNum == that.maxMigrationKeeperNum
                && srcKeeperContainer.equals(that.srcKeeperContainer)
                && targetKeeperContainer.equals(that.targetKeeperContainer)
                && migrationClusters.equals(that.migrationClusters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxMigrationKeeperNum, srcKeeperContainer, targetKeeperContainer, migrationClusters);
    }

    @Override
    public String toString() {
        return "MigrationKeeperModel{" +
                "maxMigrationKeeperNum=" + maxMigrationKeeperNum +
                ", srcKeeperContainer=" + srcKeeperContainer +
                ", targetKeeperContainer=" + targetKeeperContainer +
                ", migrationClusters=" + migrationClusters +
                '}';
    }
}
