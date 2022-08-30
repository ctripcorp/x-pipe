package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedList;
import java.util.List;

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

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<DcDetailInfo> dcDetails = new LinkedList<>();

    public static ClusterCreateInfo fromClusterTbl(ClusterTbl clusterTbl, DcService dcService, DcClusterService dcClusterService) {

        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();

        clusterCreateInfo.setClusterId(clusterTbl.getId());
        clusterCreateInfo.setDesc(clusterTbl.getClusterDescription());
        clusterCreateInfo.setClusterName(clusterTbl.getClusterName());
        clusterCreateInfo.setClusterType(clusterTbl.getClusterType());
        OrganizationTbl organizationTbl = clusterTbl.getOrganizationInfo();
        clusterCreateInfo.setOrganizationId(organizationTbl != null ? organizationTbl.getOrgId() : 0L);
        clusterCreateInfo.setClusterAdminEmails(clusterTbl.getClusterAdminEmails());

        List<DcTbl> clusterRelatedDc = dcService.findClusterRelatedDc(clusterTbl.getClusterName());
        clusterRelatedDc.forEach(dcTbl -> {

            if (dcTbl.getId() == clusterTbl.getActivedcId()) {
                clusterCreateInfo.addFirstDc(dcTbl.getDcName());
            } else {
                clusterCreateInfo.addDc(dcTbl.getDcName());
            }
        });
        if(ClusterType.isSameClusterType(clusterTbl.getClusterType(), ClusterType.HETERO)) {
            List<DcClusterTbl> dcClusterTbls = dcClusterService.findClusterRelated(clusterTbl.getId());

            dcClusterTbls.forEach(dcClusterTbl -> {
                DcDetailInfo dcDetailInfo = new DcDetailInfo()
                        .setDcId(dcClusterTbl.getDcName())
                        .setDcGroupName(dcClusterTbl.getGroupName())
                        .setDcGroupType(dcClusterTbl.isGroupType());
                if (dcClusterTbl.getDcId() == clusterTbl.getActivedcId()) {
                    clusterCreateInfo.addFirstDcDetail(dcDetailInfo);
                } else {
                    clusterCreateInfo.addDcDetail(dcDetailInfo);
                }
            });

        }

        return clusterCreateInfo;
    }

    public ClusterCreateInfo(){
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
            logger.info("[addFirstDc][already exist, remove]", clusterName);
        }
        dcs.add(0, dcName);
    }

    public void addDcDetail(DcDetailInfo dcDetailInfo) {
        boolean exist = dcDetails.stream().anyMatch(dcDetail -> dcDetail.getDcId().equals(dcDetailInfo.getDcId()));
        if(exist) {
            logger.info("[addDcDetail][already exist]{}", dcDetailInfo);
            return;
        }
        dcDetails.add(dcDetailInfo);
    }

    public void addFirstDcDetail(DcDetailInfo dcDetailInfo) {
        boolean remove = dcDetails.removeIf(dcDetail -> dcDetail.getDcId().equals(dcDetailInfo.getDcId()));
        if(remove) {
            logger.info("[addFirstDcDetail][already exist, remove]", clusterName);
        }
        dcDetails.add(0, dcDetailInfo);
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

    @Override
    public void check() throws CheckFailException{

        if(StringUtil.isEmpty(clusterName)){
            throw new CheckFailException("clusterName empty");
        }

        if(!ClusterType.isTypeValidate(clusterType)){
            throw new CheckFailException("invalidate clusterType");
        }

        if(StringUtil.isEmpty(desc)){
            throw new CheckFailException("desc empty");
        }

        if(dcs == null || dcs.size() == 0){
            throw new CheckFailException("dcs empty");
        }

        if(dcs.size() <= 1){
            throw new CheckFailException("dcs size should be at least two, first active!");
        }

        if(StringUtil.isEmpty(clusterAdminEmails)){
            throw new CheckFailException("clusterAdminEmails empty");
        }

        if(organizationId == null) {
            throw new CheckFailException("organizationId empty");
        }
    }

    @Override
    public String toString() {
        return JsonCodec.INSTANCE.encode(this);
    }
}
