package com.ctrip.xpipe.redis.console.model;

public class LeaseModel {

    private String name;

    private String owner;

    private Integer validPeriod;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Integer getValidPeriod() {
        return validPeriod;
    }

    public void setValidPeriod(Integer validPeriod) {
        this.validPeriod = validPeriod;
    }

    @Override
    public String toString() {
        return String.format("Lease{name='%s', owner='%s', validPeriod=%d}", name, owner, validPeriod);
    }
}
