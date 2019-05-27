package com.ctrip.xpipe.redis.console.model;

/**
 * @author taotaotu
 * May 24, 2019
 */
public class DcModel {

    private String dc_name;

    private long zone_id;

    private String description;

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

    @Override
    public String toString() {
        return "DcModel{" +
                "dc_name='" + dc_name + '\'' +
                ", zone_id=" + zone_id +
                ", description='" + description + '\'' +
                '}';
    }
}
