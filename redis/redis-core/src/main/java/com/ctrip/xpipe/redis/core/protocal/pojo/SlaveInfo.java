package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 07, 2017
 */
public class SlaveInfo extends AbstractInfo{

    private final static String MASTER_REPLID_PREFIX = "master_replid";
    private final static String SLAVE_REPL_OFFSET_PREFIX = "slave_repl_offset";

    private Long slaveReplOffset;
    private String masterReplId;

    public SlaveInfo(){
        this(null, null, false);
    }

    public SlaveInfo(String masterReplId, Long slaveReplOffset, boolean isKeeper){
        super(Server.SERVER_ROLE.SLAVE, isKeeper);
        this.masterReplId = masterReplId;
        this.slaveReplOffset = slaveReplOffset;
    }

    public SlaveInfo(Long slaveReplOffset, boolean isKeeper){
        this(null, slaveReplOffset, isKeeper);
    }

    public Long getSlaveReplOffset() {
        return slaveReplOffset;
    }

    public String getMasterReplId() {
        return masterReplId;
    }

    //when psync2, master -> slave, get raw master info
    public MasterInfo toMasterInfo(){

        if(masterReplId == null || slaveReplOffset == null || slaveReplOffset == 0){
            return null;
        }

        return new MasterInfo(masterReplId, slaveReplOffset);
    }


    public static SlaveInfo fromInfo(String []lines){

        SlaveInfo info = new SlaveInfo();

        for(String line : lines){
            String []sp = line.split("\\s*:\\s*");
            if(sp.length != 2){
                continue;
            }
            String key = sp[0].trim(), value = sp[1].trim();
            if(getField(key, value, info)){
                continue;
            }
            if(key.equalsIgnoreCase(MASTER_REPLID_PREFIX)){
                info.masterReplId = value;
                continue;
            }
            if(key.equalsIgnoreCase(SLAVE_REPL_OFFSET_PREFIX)){
                info.slaveReplOffset = Long.parseLong(value);
                continue;
            }
        }
        return info;
    }

}
