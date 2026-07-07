package com.ctrip.xpipe.redis.console.controller.api.dto;

import java.util.ArrayList;
import java.util.List;

public class RegionBeaconUsage {

    private List<String> dcs;
    private List<DRBeaconUsageItem> beacons;

    public RegionBeaconUsage() {
        this.dcs = new ArrayList<>();
        this.beacons = new ArrayList<>();
    }

    public List<String> getDcs() {
        return dcs;
    }

    public void setDcs(List<String> dcs) {
        this.dcs = dcs;
    }

    public List<DRBeaconUsageItem> getBeacons() {
        return beacons;
    }

    public void setBeacons(List<DRBeaconUsageItem> beacons) {
        this.beacons = beacons;
    }
}
