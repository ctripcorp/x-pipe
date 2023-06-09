package com.ctrip.xpipe.redis.console.model;

/**
 * @author taotaotu
 * May 24, 2019
 */
public class DcModel {

    private long dc_id;

    private String dc_name;

    private long zone_id;

    private String description;

    public long getDc_id() {
        return dc_id;
    }

    public DcModel setDc_id(long dc_id) {
        this.dc_id = dc_id;
        return this;
    }

    public String getDc_name() {
        return dc_name;
    }

    public void setDc_name(String dc_name) {
        this.dc_name = dc_name;
    }

    public long getZone_id() {
        return zone_id;
    }

    public void setZone_id(long zone_id) {
        this.zone_id = zone_id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public static DcModel fromDcTbl(DcTbl dcTbl) {
        DcModel dcModel = new DcModel();
        dcModel.setDc_id(dcTbl.getId());
        dcModel.setDc_name(dcTbl.getDcName());
        dcModel.setDescription(dcTbl.getDcDescription());
        dcModel.setZone_id(dcTbl.getZoneId());
        return dcModel;
    }

    @Override
    public String toString() {
        return "DcModel{" +
                "dc_id='" + dc_id + '\'' +
                "dc_name='" + dc_name + '\'' +
                ", zone_id=" + zone_id +
                ", description='" + description + '\'' +
                '}';
    }
}
