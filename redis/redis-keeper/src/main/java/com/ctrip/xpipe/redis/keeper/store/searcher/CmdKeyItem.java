package com.ctrip.xpipe.redis.keeper.store.searcher;

public class CmdKeyItem {

    String uuid;

    int seq;

    int dbId;

    String cmd;

    byte[] key;

    byte[] subkey;

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
