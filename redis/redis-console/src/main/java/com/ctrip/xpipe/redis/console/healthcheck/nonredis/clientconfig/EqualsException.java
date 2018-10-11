package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleRuntimeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public class EqualsException extends RedisConsoleRuntimeException{

    private String clusterName;
    private String shardName;

    public EqualsException(String message) {
        super(message);
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public static EqualsException createClusterException(String clusterName, String message){

        EqualsException equalsException = new EqualsException(message);
        equalsException.setClusterName(clusterName);
        return equalsException;
    }

    public static EqualsException createShardException(String shardName, String message){

        EqualsException equalsException = new EqualsException(message);
        equalsException.setShardName(shardName);
        return equalsException;
    }


    public String getShardName() {
        return shardName;
    }

    public String getClusterName() {
        return clusterName;
    }

}
