package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use= JsonTypeInfo.Id.MINIMAL_CLASS)
public interface CurrentShardMeta extends Releasable {

    void addResource(Releasable releasable);
}
