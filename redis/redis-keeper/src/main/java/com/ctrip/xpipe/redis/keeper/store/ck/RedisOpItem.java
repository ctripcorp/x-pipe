package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;

import java.util.List;

/**
 * @author TB
 * <p>
 * 2025/11/21 16:32
 */
public class RedisOpItem implements IRedisOpItem<RedisOpItem>{
    private RedisOpType redisOpType;
    private String gtid;
    private String dbId;
    private RedisKey redisKey;
    private List<RedisKey> redisKeyList;

    public RedisOpType getRedisOpType() {
        return redisOpType;
    }

    public void setRedisOpType(RedisOpType redisOpType) {
        this.redisOpType = redisOpType;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
    }

    public RedisKey getRedisKey() {
        return redisKey;
    }

    public void setRedisKey(RedisKey redisKey) {
        this.redisKey = redisKey;
    }

    public List<RedisKey> getRedisKeyList() {
        return redisKeyList;
    }

    public void setRedisKeyList(List<RedisKey> redisKeyList) {
        this.redisKeyList = redisKeyList;
    }

    @Override
    public RedisOpItem getRedisOpItem() {
        return this;
    }

    @Override
    public void clear() {
        if(redisKeyList != null) {
            redisKeyList.clear();
        }
    }
}
