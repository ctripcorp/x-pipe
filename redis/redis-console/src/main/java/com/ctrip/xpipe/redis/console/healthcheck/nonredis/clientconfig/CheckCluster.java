package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public class CheckCluster extends OuterClientService.AbstractInfo{

    private String clusterName;
    private Map<String, CheckShard> shards = new HashMap<>();

    public CheckCluster(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public CheckShard getOrCreate(String shardName) {
        return MapUtils.getOrCreate(shards, shardName, new ObjectFactory<CheckShard>() {
            @Override
            public CheckShard create() {
                return new CheckShard(shardName);
            }
        });
    }

    public String getClusterName() {
        return clusterName;
    }

    public Map<String, CheckShard> getShards() {
        return shards;
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof CheckCluster)) {
            throw EqualsException.createClusterException(clusterName, "obj not cluster:" + obj);
        }

        CheckCluster other = (CheckCluster) obj;
        if (!(ObjectUtils.equals(clusterName, other.clusterName))) {
            throw EqualsException.createClusterException(clusterName, String.format("[clusterName Not Equal]%s, %s", clusterName, other.clusterName));
        }

        try{
            if(ObjectUtils.equals(shards, other.shards)){
                return  true;
            }else {
                throw EqualsException.createClusterException(clusterName, String.format("shard not equal:%s, %s", shards, other.getShards()));
            }
        }catch (EqualsException e){
            e.setClusterName(clusterName);
            throw e;
        }
    }

    public void equalsException(Object obj) throws EqualsException{

    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(clusterName);
    }

}
