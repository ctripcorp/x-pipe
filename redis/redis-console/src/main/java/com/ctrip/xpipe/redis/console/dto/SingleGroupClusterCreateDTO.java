package com.ctrip.xpipe.redis.console.dto;

import java.util.List;

public class SingleGroupClusterCreateDTO extends ClusterCreateDTO {
    private List<String> azs;

    public SingleGroupClusterCreateDTO() {
    }

    public SingleGroupClusterCreateDTO(ClusterCreateDTO clusterCreateDTO, List<String> azs) {
        super(clusterCreateDTO);
        this.azs = azs;
    }

    public List<String> getAzs() {
        return azs;
    }

    public void setAzs(List<String> azs) {
        this.azs = azs;
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
//        private List<String> azs;
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
//        public Builder azs(List<String> val) {
//            azs = val;
//            return this;
//        }
//
//        public SingleGroupClusterCreateDTO build() {
//            return new SingleGroupClusterCreateDTO(this.clusterName, this.clusterType, this.activeAz,
//                this.description, this.orgName, this.adminEmails, this.azs);
//        }
//    }
}
