package com.ctrip.xpipe.redis.console.model.consoleportal;

/**
 * @author tt.tu
 * <p>
 *     Spet 27, 2018
 */
public class DcListDcModel {
    protected Long dcId;
    protected String dcName;
    protected String dcDescription;
    protected Integer clusterCount;
    protected Integer redisCount;
    protected Integer keeperCount;

    public DcListDcModel(Long dcId, String dcName, String dcDescription, Integer clusterCount,
                         Integer redisCount, Integer keeperCount){
        this.dcId = dcId;
        this.dcName = dcName;
        this.dcDescription = dcDescription;
        this.clusterCount = clusterCount;
        this.redisCount = redisCount;
        this.keeperCount = keeperCount;
    }

    public DcListDcModel(){}

    public Long getDcId(){
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

    public Integer getClusterCount(){
        return this.clusterCount;
    }

    public DcListDcModel setClusterCount(Integer clusterCount){
        this.clusterCount = clusterCount;
        return this;
    }
    public Integer getRedisCount(){
        return this.redisCount;
    }

    public DcListDcModel setRedisCount(Integer redisCount){
        this.redisCount = redisCount;
        return this;
    }

    public Integer getKeeperCount(){
        return this.keeperCount;
    }

    public DcListDcModel setKeeperCount(Integer keeperCount){
        this.keeperCount = keeperCount;
        return this;
    }

}
