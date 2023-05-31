package com.ctrip.xpipe.redis.console.config.model;

import java.io.Serializable;
import java.util.Objects;

public class BeaconClusterRoute implements Serializable {

    private static final long serialVersionUID = 8003837527866553091L;

    private String name;
    private String host;
    private Integer weight;

    public BeaconClusterRoute() {
    }

    public BeaconClusterRoute(String name, String host, Integer weight) {
        this.name = name;
        this.host = host;
        this.weight = weight;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
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

        BeaconClusterRoute that = (BeaconClusterRoute)o;

        if (!Objects.equals(name, that.name))
            return false;
        if (!Objects.equals(host, that.host))
            return false;
        return Objects.equals(weight, that.weight);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + (weight != null ? weight.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BeaconClusterRoute{" + "name='" + name + '\'' + ", host='" + host + '\'' + ", weight=" + weight + '}';
    }
}
