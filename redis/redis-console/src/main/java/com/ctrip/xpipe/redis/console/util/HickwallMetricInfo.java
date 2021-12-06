package com.ctrip.xpipe.redis.console.util;

/**
 * @author zhuchen
 * Apr 2020/4/10 2020
 */
public class HickwallMetricInfo {

    private String domain;

    private int delayPanelId;

    private int crossDcDelayPanelId;

    private int proxyPingPanelId;

    private int proxyTrafficPanelId;

    private int proxyCollectionPanelId;
    
    private String biDirectionClusterTemplateUrl;
    
    private String oneWayClusterTemplateUrl;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getDelayPanelId() {
        return delayPanelId;
    }

    public void setDelayPanelId(int delayPanelId) {
        this.delayPanelId = delayPanelId;
    }

    public int getProxyPingPanelId() {
        return proxyPingPanelId;
    }

    public void setProxyPingPanelId(int proxyPingPanelId) {
        this.proxyPingPanelId = proxyPingPanelId;
    }

    public int getProxyTrafficPanelId() {
        return proxyTrafficPanelId;
    }

    public void setProxyTrafficPanelId(int proxyTrafficPanelId) {
        this.proxyTrafficPanelId = proxyTrafficPanelId;
    }

    public int getProxyCollectionPanelId() {
        return proxyCollectionPanelId;
    }

    public void setProxyCollectionPanelId(int proxyCollectionPanelId) {
        this.proxyCollectionPanelId = proxyCollectionPanelId;
    }

    public int getCrossDcDelayPanelId() {
        return crossDcDelayPanelId;
    }

    public void setCrossDcDelayPanelId(int crossDcDelayPanelId) {
        this.crossDcDelayPanelId = crossDcDelayPanelId;
    }

    public String getBiDirectionClusterTemplateUrl() {
        return biDirectionClusterTemplateUrl;
    }

    public void setBiDirectionClusterTemplateUrl(String biDirectionClusterTemplateUrl) {
        this.biDirectionClusterTemplateUrl = biDirectionClusterTemplateUrl;
    }

    public String getOneWayClusterTemplateUrl() {
        return oneWayClusterTemplateUrl;
    }

    public void setOneWayClusterTemplateUrl(String oneWayClusterTemplateUrl) {
        this.oneWayClusterTemplateUrl = oneWayClusterTemplateUrl;
    }
}
