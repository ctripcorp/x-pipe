package com.ctrip.xpipe.redis.console.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LogicalBuModel implements Serializable {

    private static final long serialVersionUID = 1L;

    private long id;
    private String name;
    private String tfsFsId;
    private boolean active;
    private String description;
    private List<Long> cmsOrgIds = new ArrayList<>();
    private int keeperContainerCount;
    private List<LogicalBuKeeperContainerModel> keeperContainers = new ArrayList<>();

    public long getId() {
        return id;
    }

    public LogicalBuModel setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public LogicalBuModel setName(String name) {
        this.name = name;
        return this;
    }

    public String getTfsFsId() {
        return tfsFsId;
    }

    public LogicalBuModel setTfsFsId(String tfsFsId) {
        this.tfsFsId = tfsFsId;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public LogicalBuModel setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public LogicalBuModel setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Long> getCmsOrgIds() {
        return cmsOrgIds;
    }

    public LogicalBuModel setCmsOrgIds(List<Long> cmsOrgIds) {
        this.cmsOrgIds = cmsOrgIds;
        return this;
    }

    public int getKeeperContainerCount() {
        return keeperContainerCount;
    }

    public LogicalBuModel setKeeperContainerCount(int keeperContainerCount) {
        this.keeperContainerCount = keeperContainerCount;
        return this;
    }

    public List<LogicalBuKeeperContainerModel> getKeeperContainers() {
        return keeperContainers;
    }

    public LogicalBuModel setKeeperContainers(List<LogicalBuKeeperContainerModel> keeperContainers) {
        this.keeperContainers = keeperContainers;
        return this;
    }
}
