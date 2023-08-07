package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class KeeperContainerDetailInfo {

    private KeeperContainerMeta keeperContainer;

    private List<KeeperMeta> keeperInstances;

    public KeeperContainerDetailInfo() {
    }

    public KeeperContainerDetailInfo(KeeperContainerMeta keeperContainerMeta, ArrayList<KeeperMeta> keeperInstances) {
        this.keeperContainer = keeperContainerMeta;
        this.keeperInstances = keeperInstances;
    }

    public KeeperContainerMeta getKeeperContainer() {
        return keeperContainer;
    }

    public KeeperContainerDetailInfo setKeeperContainer(KeeperContainerMeta keeperContainer) {
        this.keeperContainer = keeperContainer;
        return this;
    }

    public List<KeeperMeta> getKeeperInstances() {
        return keeperInstances;
    }

    public KeeperContainerDetailInfo setKeeperInstances(List<KeeperMeta> keeperInstances) {
        this.keeperInstances = keeperInstances;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeeperContainerDetailInfo that = (KeeperContainerDetailInfo) o;
        return Objects.equals(keeperContainer, that.keeperContainer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keeperContainer);
    }
}
