package com.ctrip.xpipe.redis.console.config.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class BeaconOrgRoute implements Serializable {

    private static final long serialVersionUID = -2118300336327416574L;

    @JsonProperty("org_id")
    private Long orgId;
    @JsonProperty("cluster_routes")
    private List<BeaconClusterRoute> clusterRoutes = Lists.newArrayList();
    private Integer weight;

    public BeaconOrgRoute() {
    }

    public BeaconOrgRoute(Long orgId, List<BeaconClusterRoute> clusterRoutes, Integer weight) {
        this.orgId = orgId;
        this.clusterRoutes = clusterRoutes;
        this.weight = weight;
    }

    public Long getOrgId() {
        return orgId;
    }

    public void setOrgId(Long orgId) {
        this.orgId = orgId;
    }

    public List<BeaconClusterRoute> getClusterRoutes() {
        return clusterRoutes;
    }

    public void setClusterRoutes(List<BeaconClusterRoute> clusterRoutes) {
        this.clusterRoutes = clusterRoutes;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BeaconOrgRoute orgRoute = (BeaconOrgRoute)o;

        if (!Objects.equals(orgId, orgRoute.orgId))
            return false;
        if (!Objects.equals(clusterRoutes, orgRoute.clusterRoutes))
            return false;
        return Objects.equals(weight, orgRoute.weight);
    }

    @Override
    public int hashCode() {
        int result = orgId != null ? orgId.hashCode() : 0;
        result = 31 * result + (clusterRoutes != null ? clusterRoutes.hashCode() : 0);
        result = 31 * result + (weight != null ? weight.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BeaconOrgRoute{" + "orgId=" + orgId + ", clusterRoutes=" + clusterRoutes + ", weight=" + weight + '}';
    }
}
