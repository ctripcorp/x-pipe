package com.ctrip.xpipe.redis.console.health.clientconfig;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.HashSet;
import java.util.Set;


/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public class CheckShard extends OuterClientService.AbstractInfo{

    private String shardName;
    private Set<CheckRedis> redises = new HashSet<>();

    public CheckShard(String shardName) {
        this.shardName = shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public void addRedis(CheckRedis checkRedis) {
        redises.add(checkRedis);
    }

    public String getShardName() {
        return shardName;
    }

    public Set<CheckRedis> getRedises() {
        return redises;
    }

    public void clearRedises(){
        redises.clear();
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(shardName);
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof CheckShard)) {
            throw  EqualsException.createShardException(shardName, "obj not shard:" + obj);
        }

        CheckShard other = (CheckShard) obj;
        if (!ObjectUtils.equals(shardName, other.shardName)) {
            throw EqualsException.createShardException(shardName, String.format("[shardName Not Equal]%s, %s", shardName, other.shardName));
        }

        try{
            if(ObjectUtils.equals(redises, other.redises)){
                return true;
            }else {
                throw EqualsException.createShardException(shardName, String.format("[redises not equal], \n%s, \n%s", redises, other.redises));
            }
        }catch (EqualsException e){
            throw EqualsException.createShardException(shardName, String.format("[redises not equal], \n%s, \n%s", redises, other.redises));
        }
    }

}
