package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CurrentShardKeeperMeta extends AbstractCurrentShardInstanceMeta {

    private AtomicBoolean watched = new AtomicBoolean(false);
    private List<KeeperMeta> surviveKeepers = new LinkedList<>();
    private Pair<String, Integer> keeperMaster;

    public CurrentShardKeeperMeta(@JsonProperty("clusterDbId") Long clusterDbId, @JsonProperty("shardDbId") Long shardDbId) {
        super(clusterDbId, shardDbId);
    }

    public boolean watchIfNotWatched() {
        return watched.compareAndSet(false, true);
    }

    @JsonIgnore
    public boolean setActiveKeeper(KeeperMeta activeKeeper) {

        if (!checkIn(surviveKeepers, activeKeeper)) {
            throw new IllegalArgumentException(
                    "active not in all survivors " + activeKeeper + ", all:" + this.surviveKeepers);
        }
        return doSetActive(activeKeeper);
    }

    @JsonIgnore
    public KeeperMeta getActiveKeeper() {
        for (KeeperMeta survive : surviveKeepers) {
            if (survive.isActive()) {
                return survive;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public List<KeeperMeta> getSurviveKeepers() {
        return MetaCloneFacade.INSTANCE.cloneList(surviveKeepers);
    }

    @SuppressWarnings("unchecked")
    public void setSurviveKeepers(List<KeeperMeta> surviveKeepers, KeeperMeta activeKeeper) {

        if (surviveKeepers.size() > 0) {
            if (!checkIn(surviveKeepers, activeKeeper)) {
                throw new IllegalArgumentException(
                        "active not in all survivors " + activeKeeper + ", all:" + this.surviveKeepers);
            }
            this.surviveKeepers = MetaCloneFacade.INSTANCE.cloneList(surviveKeepers);
            logger.info("[setSurviveKeepers]cluster_{},shard_{},{}, {}", clusterDbId, shardDbId, surviveKeepers, activeKeeper);
            doSetActive(activeKeeper);
        } else {
            logger.info("[setSurviveKeepers][survive keeper none, clear]cluster{},shard_{},{}, {}", clusterDbId, shardDbId,
                    surviveKeepers, activeKeeper);
            this.surviveKeepers.clear();
        }
    }

    private boolean doSetActive(KeeperMeta activeKeeper) {

        boolean changed = false;
        logger.info("[doSetActive]cluster_{},shard_{},{}", clusterDbId, shardDbId, activeKeeper);
        for (KeeperMeta survive : this.surviveKeepers) {

            if (MetaUtils.same(survive, activeKeeper)) {
                if (!survive.isActive()) {
                    survive.setActive(true);
                    changed = true;
                }
            } else {
                if (survive.isActive()) {
                    survive.setActive(false);
                }
            }
        }
        return changed;
    }

    private boolean checkIn(List<KeeperMeta> surviveKeepers, KeeperMeta activeKeeper) {
        for (KeeperMeta survive : surviveKeepers) {
            if (MetaUtils.same(survive, activeKeeper)) {
                return true;
            }
        }
        return false;
    }

    public Pair<String, Integer> getKeeperMaster() {

        if(keeperMaster == null){
            return null;
        }
        return new Pair<String, Integer>(keeperMaster.getKey(), keeperMaster.getValue());
    }

    public synchronized boolean setKeeperMaster(Pair<String, Integer> keeperMaster) {

        logger.info("[setKeeperMaster]cluster_{},shard_{},{}", clusterDbId, shardDbId, keeperMaster);
        if (ObjectUtils.equals(this.keeperMaster, keeperMaster)) {
            return false;
        }

        if(keeperMaster == null){
            this.keeperMaster = null;
        }else{
            this.keeperMaster = new Pair<String, Integer>(keeperMaster.getKey(), keeperMaster.getValue());
        }
        return true;
    }

}
