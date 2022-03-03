package com.ctrip.xpipe.redis.console.model.consoleportal;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tt.tu
 * <p>
 *     Spet 27, 2018
 */
public class DcListDcModel {
    protected long dcId;
    protected String dcName;
    protected String dcDescription;
    private List<DcClusterTypeStatisticsModel> dcClusterTypes = new ArrayList<>();

    public DcListDcModel(long dcId, String dcName, String dcDescription){
        this.dcId = dcId;
        this.dcName = dcName;
        this.dcDescription = dcDescription;
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

    public List<DcClusterTypeStatisticsModel> getClusterTypes() {
        return dcClusterTypes;
    }

    public DcListDcModel setClusterTypes(List<DcClusterTypeStatisticsModel> dcClusterTypeStatisticsModels) {
        this.dcClusterTypes = dcClusterTypeStatisticsModels;
        return this;
    }
}
