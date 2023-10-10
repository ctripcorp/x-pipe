package com.ctrip.xpipe.redis.console.dto;

import java.util.List;

public class MultiGroupClusterCreateDTO extends ClusterCreateDTO {

    public MultiGroupClusterCreateDTO() {
    }

    public MultiGroupClusterCreateDTO(ClusterCreateDTO clusterCreateDTO, List<AzGroupDTO> azGroups) {
        super(clusterCreateDTO);
        this.azGroups = azGroups;
    }

    private List<AzGroupDTO> azGroups;

    public List<AzGroupDTO> getAzGroups() {
        return azGroups;
    }

    public void setAzGroups(List<AzGroupDTO> azGroups) {
        this.azGroups = azGroups;
    }

//    public static Builder builder() {
//        return new Builder();
//    }
//
//    public static final class Builder {
//        private String clusterName;
//        private String clusterType;
//        private String activeAz;
//        private String description;
//        private String orgName;
//        private String adminEmails;
//        private List<AzGroupDTO> azGroups;
//
//        public Builder() {
//        }
//
//        public Builder clusterName(String val) {
//            clusterName = val;
//            return this;
//        }
//
//        public Builder clusterType(String val) {
//            clusterType = val;
//            return this;
//        }
//
//        public Builder activeAz(String val) {
//            activeAz = val;
//            return this;
//        }
//
//        public Builder description(String val) {
//            description = val;
//            return this;
//        }
//
//        public Builder orgName(String val) {
//            orgName = val;
//            return this;
//        }
//
//        public Builder adminEmails(String val) {
//            adminEmails = val;
//            return this;
//        }
//
//        public Builder azGroups(List<AzGroupDTO> val) {
//            azGroups = val;
//            return this;
//        }
//
//        public MultiGroupClusterCreateDTO build() {
//            return new MultiGroupClusterCreateDTO(this.clusterName, this.clusterType, this.activeAz,
//                this.description, this.orgName, this.adminEmails, this.azGroups);
//        }
//    }
}
