package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use= JsonTypeInfo.Id.MINIMAL_CLASS)
public interface CurrentShardMeta extends Releasable {

    boolean watchIfNotWatched();

    void addResource(Releasable releasable);

    Long getClusterDbId();

    Long getShardDbId();

}
