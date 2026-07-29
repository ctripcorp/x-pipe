package com.ctrip.xpipe.redis.console.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AzGroupModel {

    private Long id;
    private String name;
    private Set<String> azs;

    public AzGroupModel() {}

    public AzGroupModel(Long id, String name, Collection<String> azs) {
        this.id = id;
        this.name = name;
        this.azs = new HashSet<>(azs);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<String> getAzs() {
        return azs;
    }

    public void setAzs(Set<String> azs) {
        this.azs = azs;
    }

    public List<String> getAzsAsList() {
        return new ArrayList<>(azs);
    }

    public boolean containsAz(String az) {
        return this.azs.contains(az);
    }
}
