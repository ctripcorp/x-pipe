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
    
    private int outComingTrafficToPeerPanelId;
    
    private int inComingTrafficFromPeerPanelId;
    
    private int peerSyncFullPanelId;
    
    private int peerSyncPartialPanelId;

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

    public int getOutComingTrafficToPeerPanelId() {
        return outComingTrafficToPeerPanelId;
    }

    public void setOutComingTrafficToPeerPanelId(int outComingTrafficToPeerPanelId) {
        this.outComingTrafficToPeerPanelId = outComingTrafficToPeerPanelId;
    }

    public int getInComingTrafficFromPeerPanelId() {
        return inComingTrafficFromPeerPanelId;
    }

    public void setInComingTrafficFromPeerPanelId(int inComingTrafficFromPeerPanelId) {
        this.inComingTrafficFromPeerPanelId = inComingTrafficFromPeerPanelId;
    }

    public int getPeerSyncFullPanelId() {
        return peerSyncFullPanelId;
    }

    public void setPeerSyncFullPanelId(int peerSyncFullPanelId) {
        this.peerSyncFullPanelId = peerSyncFullPanelId;
    }

    public int getPeerSyncPartialPanelId() {
        return peerSyncPartialPanelId;
    }

    public void setPeerSyncPartialPanelId(int peerSyncPartialPanelId) {
        this.peerSyncPartialPanelId = peerSyncPartialPanelId;
    }
}
