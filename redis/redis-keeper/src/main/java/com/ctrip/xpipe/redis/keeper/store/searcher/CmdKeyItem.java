package com.ctrip.xpipe.redis.keeper.store.searcher;

import com.ctrip.xpipe.api.codec.RawByteArraySerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class CmdKeyItem {

    public String uuid;

    public int seq;

    public int dbId;

    public String cmd;

    @JsonSerialize(using = RawByteArraySerializer.class)
    public byte[] key;

    @JsonSerialize(using = RawByteArraySerializer.class)
    public byte[] subkey;

    public CmdKeyItem(String uuid, int seq, int dbId, String cmd, byte[] key, byte[] subkey) {
        this.uuid = uuid;
        this.seq = seq;
        this.dbId = dbId;
        this.cmd = cmd;
        this.key = key;
        this.subkey = subkey;
    }

    public CmdKeyItem(String uuid, int seq, int dbId, String cmd, byte[] key) {
        this(uuid, seq, dbId, cmd, key, null);
    }

}
