package com.ctrip.xpipe.redis.console.model.consoleportal;

/**
 * @author tt.tu
 * <p>
 *     Spet 27, 2018
 */
public class DcListDcModel {
    protected long dcId;
    protected String dcName;
    protected String dcDescription;
    protected int clusterCount;
    protected int redisCount;
    protected int keeperCount;
    protected int keeperContainerCount;
    protected int clusterInActiveDcCount;

    public DcListDcModel(long dcId, String dcName, String dcDescription, int clusterCount,
                         int redisCount, int keeperCount, int keeperContainerCount, int clusterInActiveDcCount){
        this.dcId = dcId;
        this.dcName = dcName;
        this.dcDescription = dcDescription;
        this.clusterCount = clusterCount;
        this.redisCount = redisCount;
        this.keeperCount = keeperCount;
        this.keeperContainerCount = keeperContainerCount;
        this.clusterInActiveDcCount = clusterInActiveDcCount;
    }

    public DcListDcModel(){}

    public long getDcId(){
        return this.dcId;
    }

    public DcListDcModel setDcId(Long dcId){
        this.dcId = dcId;
        return this;
    }

    public String getDcName() {
        return dcName;
    }

    public DcListDcModel setDcName(String dcName){
        this.dcName = dcName;
        return this;
    }

    public String getDcDescription() {
        return dcDescription;
    }

    public DcListDcModel setDcDescription(String dcDescription){
        this.dcDescription = dcDescription;
        return this;
    }

    public int getClusterCount(){
        return this.clusterCount;
    }

    public DcListDcModel setClusterCount(Integer clusterCount){
        this.clusterCount = clusterCount;
        return this;
    }
    public int getRedisCount(){
        return this.redisCount;
    }

    public DcListDcModel setRedisCount(Integer redisCount){
        this.redisCount = redisCount;
        return this;
    }

    public int getKeeperCount(){
        return this.keeperCount;
    }

    public DcListDcModel setKeeperCount(Integer keeperCount){
        this.keeperCount = keeperCount;
        return this;
    }

    public int getKeeperContainerCount(){return this.keeperContainerCount;}

    public DcListDcModel setKeeperContainerCount(Integer keeperContainerCount){
        this.keeperContainerCount = keeperContainerCount;
        return this;
    }

    public int getClusterInActiveDcCount(){return this.clusterInActiveDcCount;}

    public DcListDcModel setClusterInActiveDcCount(Integer clusterInActiveDcCount){
        this.clusterInActiveDcCount = clusterInActiveDcCount;
        return this;
    }

}
