package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class MigrationKeeperContainerDetailModel implements Serializable {

    private KeeperContainerUsedInfoModel srcKeeperContainer;

    private KeeperContainerUsedInfoModel targetKeeperContainer;

    private int migrateKeeperCount = 0;

    private int migrateKeeperCompleteCount = 0;

    List<DcClusterShard> migrateShards;

    public MigrationKeeperContainerDetailModel() {

    }

    public MigrationKeeperContainerDetailModel(KeeperContainerUsedInfoModel srcKeeperContainer, KeeperContainerUsedInfoModel targetKeeperContainer,
                                               int migrateKeeperCount, List<DcClusterShard> migrateShards) {
        this.srcKeeperContainer = srcKeeperContainer;
        this.targetKeeperContainer = targetKeeperContainer;
        this.migrateKeeperCount = migrateKeeperCount;
        this.migrateShards = migrateShards;
    }

    public KeeperContainerUsedInfoModel getSrcKeeperContainer() {
        return srcKeeperContainer;
    }

    public MigrationKeeperContainerDetailModel setSrcKeeperContainer(KeeperContainerUsedInfoModel srcKeeperContainer) {
        this.srcKeeperContainer = srcKeeperContainer;
        return this;
    }

    public KeeperContainerUsedInfoModel getTargetKeeperContainer() {
        return targetKeeperContainer;
    }

    public MigrationKeeperContainerDetailModel setTargetKeeperContainer(KeeperContainerUsedInfoModel targetKeeperContainer) {
        this.targetKeeperContainer = targetKeeperContainer;
        return this;
    }

    public int getMigrateKeeperCount() {
        return migrateKeeperCount;
    }

    public MigrationKeeperContainerDetailModel setMigrateKeeperCount(int migrateKeeperCount) {
        this.migrateKeeperCount = migrateKeeperCount;
        return this;
    }

    public List<DcClusterShard> getMigrateShards() {
        return migrateShards;
    }

    public MigrationKeeperContainerDetailModel setMigrateShards(List<DcClusterShard> migrateShards) {
        this.migrateShards = migrateShards;
        return this;
    }

    public void migrateKeeperCountIncrease() {
        this.migrateKeeperCount++;
    }

    public void migrateKeeperCompleteCountIncrease() {
        this.migrateKeeperCompleteCount++;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationKeeperContainerDetailModel that = (MigrationKeeperContainerDetailModel) o;
        return srcKeeperContainer.equals(that.srcKeeperContainer) && targetKeeperContainer.equals(that.targetKeeperContainer);
    }

    public void setMigrateKeeperCompleteCount(int migrateKeeperCompleteCount) {
        this.migrateKeeperCompleteCount = migrateKeeperCompleteCount;
    }

    public int getMigrateKeeperCompleteCount() {
        return migrateKeeperCompleteCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcKeeperContainer, targetKeeperContainer);
    }
}
