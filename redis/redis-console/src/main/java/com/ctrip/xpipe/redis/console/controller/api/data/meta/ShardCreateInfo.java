package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public class ShardCreateInfo extends AbstractCreateInfo{

    protected String shardName;

    protected String shardMonitorName;

    public ShardCreateInfo(){

    }

    public ShardCreateInfo(String shardName, String shardMonitorName){
        this.shardName = shardName;
        this.shardMonitorName = shardMonitorName;
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public String getShardMonitorName() {
        return shardMonitorName;
    }

    public void setShardMonitorName(String shardMonitorName) {
        this.shardMonitorName = shardMonitorName;
    }

    @Override
    public void check() throws CheckFailException{

        if(StringUtil.isEmpty(shardName)){
            throw new CheckFailException("shardName empty");
        }

        if(StringUtil.isEmpty(shardMonitorName)){
            throw new CheckFailException("shardMonitorName empty");
        }
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
