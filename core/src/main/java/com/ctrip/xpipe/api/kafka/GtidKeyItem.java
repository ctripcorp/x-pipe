package com.ctrip.xpipe.api.kafka;

public class GtidKeyItem {
    private String cmd;
    private String uuid;
    private String seq;
    private byte[] key;
    private byte[] subkey;
    private String dbid;
    private long shardid;
    private String address;

    public GtidKeyItem(String cmd,String uuid, String seq, byte[] key, byte[] subkey, String dbid, long shardid,String address){
        this.cmd = cmd;
        this.uuid = uuid;
        this.seq = seq;
        this.key = key;
        this.subkey = subkey;
        this.dbid = dbid;
        this.shardid = shardid;
        this.address = address;
    }

    public String getCmd() {
        return cmd;
    }

    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSeq() {
        return seq;
    }

    public void setSeq(String seq) {
        this.seq = seq;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getSubkey() {
        return subkey;
    }

    public void setSubkey(byte[] subkey) {
        this.subkey = subkey;
    }

    public String getDbid() {
        return dbid;
    }

    public void setDbid(String dbid) {
        this.dbid = dbid;
    }

    public long getShardid() {
        return shardid;
    }

    public void setShardid(long shardid) {
        this.shardid = shardid;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
