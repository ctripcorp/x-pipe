package com.ctrip.xpipe.redis.core.metaserver.model;

import com.ctrip.xpipe.redis.core.entity.ShardMeta;

public class ShardAllMetaModel {

    private String MetaHost;
    private int MetaPort;
    private ShardCurrentMetaModel shardCurrentMeta;
    private Throwable err;

    public String getMetaHost() {
        return MetaHost;
    }

    public ShardAllMetaModel setMetaHost(String metaHost) {
        MetaHost = metaHost;
        return this;
    }

    public int getMetaPort() {
        return MetaPort;
    }

    public ShardAllMetaModel setMetaPort(int metaPort) {
        MetaPort = metaPort;
        return this;
    }

    public ShardCurrentMetaModel getShardCurrentMeta() {
        return shardCurrentMeta;
    }

    public ShardAllMetaModel setShardCurrentMeta(ShardCurrentMetaModel shardCurrentMeta) {
        this.shardCurrentMeta = shardCurrentMeta;
        return this;
    }

    public Throwable getErr() {
        return err;
    }

    public void setErr(Throwable err) {
        this.err = err;
    }
}
