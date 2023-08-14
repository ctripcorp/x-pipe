package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;
import java.util.Objects;

public class KeeperContainerDetailInfo {

    private KeeperContainerMeta keeperContainer;

    private List<KeeperMeta> keeperInstances;

    private List<RedisMeta> redisInstances;

    public KeeperContainerDetailInfo() {
    }

    public KeeperContainerDetailInfo(KeeperContainerMeta keeperContainer, List<KeeperMeta> keeperInstances, List<RedisMeta> redisInstances) {
        this.keeperContainer = keeperContainer;
        this.keeperInstances = keeperInstances;
        this.redisInstances = redisInstances;
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

    public List<RedisMeta> getRedisInstances() {
        return redisInstances;
    }

    public KeeperContainerDetailInfo setRedisInstances(List<RedisMeta> redisInstances) {
        this.redisInstances = redisInstances;
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
