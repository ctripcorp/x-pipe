package com.ctrip.xpipe.redis.core.protocal.pojo;

import com.ctrip.xpipe.api.server.Server;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 07, 2017
 */
public class MasterInfo extends AbstractInfo{

    private static final String MASTER_REPL_OFFSET_PREFIX = "master_repl_offset";
    private static final String MASTER_REPLID_PREFIX = "master_replid";

    private Long masterReplOffset;

    private String replId;

    public MasterInfo(){
        this(null, null);
    }

    public MasterInfo(String replId, Long masterReplOffset){
        super(Server.SERVER_ROLE.MASTER, false);
        this.replId = replId;
        this.masterReplOffset = masterReplOffset;
    }

    public MasterInfo(Long masterReplOffset){
        this(null, masterReplOffset);
    }


    public Long getMasterReplOffset() {
        return masterReplOffset;
    }

    public String getReplId() {
        return replId;
    }

    public void setReplId(String replId) {
        this.replId = replId;
    }


    public static MasterInfo fromInfo(String []lines){

        MasterInfo masterInfo = new MasterInfo();

        for(String line : lines){
            String []sp = line.split("\\s*:\\s*");
            if(sp.length != 2){
                continue;
            }
            String key = sp[0].trim(), value = sp[1].trim();
            if(getField(key, value, masterInfo)){
                continue;
            }
            if (key.equalsIgnoreCase(MASTER_REPL_OFFSET_PREFIX)) {
                masterInfo.masterReplOffset = Long.parseLong(value);
                continue;
            }
            if (key.equalsIgnoreCase(MASTER_REPLID_PREFIX)) {
                masterInfo.replId = value;
                continue;
            }
        }
        return masterInfo;
    }
}
