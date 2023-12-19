package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author ayq
 * <p>
 * 2022/4/6 21:26
 */
public class CurrentShardApplierMeta extends AbstractCurrentShardInstanceMeta {

    private AtomicBoolean watched = new AtomicBoolean(false);
    private List<ApplierMeta> surviveAppliers = new LinkedList<>();
    private Pair<String, Integer> applierMaster;
    private String srcSids;

    public CurrentShardApplierMeta(@JsonProperty("clusterDbId") Long clusterDbId, @JsonProperty("shardDbId") Long shardDbId) {
        super(clusterDbId, shardDbId);
    }

    public boolean watchIfNotWatched() {
        return watched.compareAndSet(false, true);
    }

    @JsonIgnore
    public boolean setActiveApplier(ApplierMeta activeApplier) {
        if (!checkIn(surviveAppliers, activeApplier)) {
            throw new IllegalArgumentException(
                    "active not in all survivors " + activeApplier + ", all:" + this.surviveAppliers);
        }
        return doSetActive(activeApplier);
    }

    @JsonIgnore
    public ApplierMeta getActiveApplier() {
        for (ApplierMeta survive : surviveAppliers) {
            if (survive.isActive()) {
                return survive;
            }
        }
        return null;
    }

    public String getSrcSids() {
        return srcSids;
    }

    public synchronized boolean setSrcSids(String srcSids) {
        logger.info("[setSrcSids]cluster_{},shard_{},{}", clusterDbId, shardDbId, srcSids);

        Set<String> sidSet = StringUtils.isEmpty(srcSids)?
                new HashSet<>():
                new HashSet<>(Arrays.asList(srcSids.split(",")));

        Set<String> currentSidSet = StringUtils.isEmpty(this.srcSids)?
                new HashSet<>():
                new HashSet<>(Arrays.asList(this.srcSids.split(",")));

        if (!sidSet.retainAll(currentSidSet) && !currentSidSet.retainAll(sidSet)) {
            return false;
        }

        this.srcSids = srcSids;
        return true;
    }

    @SuppressWarnings("unchecked")
    public List<ApplierMeta> getSurviveAppliers() {
        return MetaCloneFacade.INSTANCE.cloneList(surviveAppliers);
    }

    @SuppressWarnings("unchecked")
    public void setSurviveAppliers(List<ApplierMeta> surviveAppliers, ApplierMeta activeApplier) {

        if (surviveAppliers.size() > 0) {
            if (!checkIn(surviveAppliers, activeApplier)) {
                throw new IllegalArgumentException(
                        "active not in all survivors " + activeApplier + ", all:" + this.surviveAppliers);
            }
            this.surviveAppliers = MetaCloneFacade.INSTANCE.cloneList(surviveAppliers);
            logger.info("[setSurviveAppliers]cluster_{},shard_{},{}, {}", clusterDbId, shardDbId, surviveAppliers, activeApplier);
            doSetActive(activeApplier);
        } else {
            logger.info("[setSurviveAppliers][survive applier none, clear]cluster{},shard_{},{}, {}", clusterDbId, shardDbId,
                    surviveAppliers, activeApplier);
            this.surviveAppliers.clear();
        }
    }

    public Pair<String, Integer> getApplierMaster() {

        if(applierMaster == null){
            return null;
        }
        return new Pair<String, Integer>(applierMaster.getKey(), applierMaster.getValue());
    }

    public synchronized boolean setApplierMaster(Pair<String, Integer> applierMaster) {

        logger.info("[setApplierMaster]cluster_{},shard_{},{}", clusterDbId, shardDbId, applierMaster);
        if (ObjectUtils.equals(this.applierMaster, applierMaster)) {
            return false;
        }

        if(applierMaster == null){
            this.applierMaster = null;
        }else{
            this.applierMaster = new Pair<String, Integer>(applierMaster.getKey(), applierMaster.getValue());
        }
        return true;
    }

    private boolean checkIn(List<ApplierMeta> surviveAppliers, ApplierMeta activeApplier) {
        for (ApplierMeta survive : surviveAppliers) {
            if (MetaUtils.same(survive, activeApplier)) {
                return true;
            }
        }
        return false;
    }

    private boolean doSetActive(ApplierMeta activeApplier) {

        boolean changed = false;
        logger.info("[doSetActive]cluster_{},shard_{},{}", clusterDbId, shardDbId, activeApplier);
        for (ApplierMeta survive : this.surviveAppliers) {

            if (MetaUtils.same(survive, activeApplier)) {
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
}
