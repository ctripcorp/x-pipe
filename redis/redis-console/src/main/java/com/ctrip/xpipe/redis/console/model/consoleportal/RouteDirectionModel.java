package com.ctrip.xpipe.redis.console.model.consoleportal;

import java.util.ArrayList;
import java.util.List;

public class RouteDirectionModel {

    private String srcDcName = "";

    private String destDcName = "";

    private int activeRouteNum = 0;

    private int publicRouteNum = 0;

    private List<RouteInfoModel> routes = new ArrayList<>();

    public RouteDirectionModel(String srcDcName, String destDcName) {
        this.srcDcName = srcDcName;
        this.destDcName = destDcName;
    }

    public RouteDirectionModel activeRouteNumIncrement() {
        activeRouteNum++;
        return this;
    }

    public RouteDirectionModel publicRouteNumIncrement() {
        publicRouteNum++;
        return this;
    }

    public List<RouteInfoModel> getRoutes() {
        return routes;
    }

    public RouteDirectionModel setRoutes(List<RouteInfoModel> routes) {
        this.routes = routes;
        return this;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public RouteDirectionModel setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
        return this;
    }

    public String getDestDcName() {
        return destDcName;
    }

    public RouteDirectionModel setDestDcName(String destDcName) {
        this.destDcName = destDcName;
        return this;
    }

    public int getActiveRouteNum() {
        return activeRouteNum;
    }

    public RouteDirectionModel setActiveRouteNum(int activeRouteNum) {
        this.activeRouteNum = activeRouteNum;
        return this;
    }

    public int getPublicRouteNum() {
        return publicRouteNum;
    }

    public RouteDirectionModel setPublicRouteNum(int publicRouteNum) {
        this.publicRouteNum = publicRouteNum;
        return this;
    }


}
