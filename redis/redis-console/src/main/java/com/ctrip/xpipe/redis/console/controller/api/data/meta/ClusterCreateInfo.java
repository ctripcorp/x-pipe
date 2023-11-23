package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.dto.ClusterDTO;
import com.ctrip.xpipe.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public class ClusterCreateInfo extends AbstractCreateInfo{

    private Long clusterId;

    private String clusterName;

    private String clusterType = ClusterType.ONE_WAY.toString();

    private List<String> dcs = new LinkedList<>();

    private String desc;

    private Long organizationId;

    private String clusterAdminEmails;

    @Deprecated
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<DcDetailInfo> dcDetails = new LinkedList<>();

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RegionInfo> regions = new LinkedList<>();

    public ClusterCreateInfo(){
    }

    public ClusterCreateInfo(ClusterDTO clusterDTO) {
        this.clusterId = clusterDTO.getClusterId();
        this.clusterName = clusterDTO.getClusterName();
        this.clusterType = clusterDTO.getClusterType();
        this.desc = clusterDTO.getDescription();
        this.organizationId = clusterDTO.getCmsOrgId();
        this.clusterAdminEmails = clusterDTO.getAdminEmails();
        this.dcs = new ArrayList<>();
        for (String az : clusterDTO.getAzs()) {
            if (Objects.equals(az, clusterDTO.getActiveAz())) {
                this.dcs.add(0, az);
            } else {
                this.dcs.add(az);
            }
        }
        if (!CollectionUtils.isEmpty(clusterDTO.getAzGroups())) {
            this.regions = clusterDTO.getAzGroups().stream().map(azGroup -> {
                RegionInfo regionInfo = new RegionInfo();
                regionInfo.setRegion(azGroup.getRegion());
                regionInfo.setClusterType(azGroup.getClusterType());
                regionInfo.setActiveAz(azGroup.getActiveAz());
                List<String> azs = new ArrayList<>();
                for (String az : azGroup.getAzs()) {
                    if (Objects.equals(az, azGroup.getActiveAz())) {
                        azs.add(0, az);
                    } else {
                        azs.add(az);
                    }
                }
                regionInfo.setAzs(azs);
                return regionInfo;
            }).collect(Collectors.toList());
        }

    }

    public Long getClusterId() {
        return clusterId;
    }

    public void setClusterId(Long clusterId) {
        this.clusterId = clusterId;
    }

    public Long getOrganizationId() {
        return organizationId;
    }

    public void setOrganizationId(Long organizationId) {
        this.organizationId = organizationId;
    }

    public String getClusterAdminEmails() {
        return clusterAdminEmails;
    }

    public void setClusterAdminEmails(String clusterAdminEmails) {
        this.clusterAdminEmails = clusterAdminEmails;
    }

    public List<String> getDcs() {
        return dcs;
    }

    public void setDcs(List<String> dcs) {
        this.dcs = dcs;
    }

    public void addDc(String dcName){
        if(dcs.contains(dcName)){
            logger.info("[addDc][already exist]{}", dcName);
            return;
        }
        dcs.add(dcName);
    }

    public void addFirstDc(String dcName){
        boolean remove = dcs.remove(dcName);
        if(remove){
            logger.info("[addFirstDc][{} already exist, remove]", clusterName);
        }
        dcs.add(0, dcName);
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterType() {
        return clusterType;
    }

    public void setClusterType(String clusterType) {
        this.clusterType = clusterType;
    }

    public List<DcDetailInfo> getDcDetails() {
        return dcDetails;
    }

    public ClusterCreateInfo setDcDetails(List<DcDetailInfo> dcDetails) {
        this.dcDetails = dcDetails;
        return this;
    }

    public List<RegionInfo> getRegions() {
        return regions;
    }

    public void setRegions(List<RegionInfo> regions) {
        this.regions = regions;
    }

    @Override
    public void check() throws CheckFailException {
        if (StringUtil.isEmpty(clusterName)) {
            throw new CheckFailException("clusterName empty");
        }

        if (!ClusterType.isTypeValidate(clusterType)) {
            throw new CheckFailException("invalidate clusterType");
        }

        if (StringUtil.isEmpty(desc)) {
            throw new CheckFailException("desc empty");
        }

        if (StringUtil.isEmpty(clusterAdminEmails)) {
            throw new CheckFailException("clusterAdminEmails empty");
        }

        if (organizationId == null) {
            throw new CheckFailException("organizationId empty");
        }

        if (CollectionUtils.isEmpty(dcs) && CollectionUtils.isEmpty(regions)) {
            throw new CheckFailException("dcs, regions should have at least one not empty");
        }

        if (CollectionUtils.isEmpty(regions) && dcs.size() == 1) {
            throw new CheckFailException("dcs size should be at least two, first active!");
        }
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
