package com.ctrip.xpipe.redis.core.store.ck;

import java.util.List;

public class GtidKeyItem {
    private String uuid;
    private String seq;
    private byte[] key;
    private String subkey;
    private String dbid;
    private long timestamp;
    private String shardid;
    private List<Integer> rediskey;

    public GtidKeyItem(String uuid,String seq,byte[] key,String subkey,String dbid,String shardid,List<Integer> rediskey){
        this.uuid = uuid;
        this.seq = seq;
        this.key = key;
        this.subkey = subkey;
        this.dbid = dbid;
        this.shardid = shardid;
//        this.rediskey = rediskey;
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

    public String getSubkey() {
        return subkey;
    }

    public void setSubkey(String subkey) {
        this.subkey = subkey;
    }

    public String getDbid() {
        return dbid;
    }

    public void setDbid(String dbid) {
        this.dbid = dbid;
    }

    public String getShardid() {
        return shardid;
    }

    public void setShardid(String shardid) {
        this.shardid = shardid;
    }

    public List<Integer> getRediskey() {
        return rediskey;
    }

    public void setRediskey(List<Integer> rediskey) {
        this.rediskey = rediskey;
    }
}
