package com.ctrip.xpipe.redis.console.health.clientconfig;

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

    @Override
    public String getMessage() {

        return clusterShardDesc() + "," + super.getMessage();
    }

    public String simpleMessage(){
        return clusterShardDesc();
    }

    private String clusterShardDesc() {

        String msg = "";
        if(clusterName != null){
            msg += "cluster:" + clusterName;
        }
        if(shardName != null){
            msg += ", shard:" + shardName;
        }
        return msg;
    }
}
