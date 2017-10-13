package com.ctrip.xpipe.service.organization;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public class Data {
    private long organizationId;
    private String name;
    private String englishName;
    private String code;
    private String createTime;
    private String cp4code;
    private String sbu;
    private String cost_center;
    private String description;
    private String status;
    private String domainNameKeywords;

    public long getOrganizationId() {
        return organizationId;
    }

    public void setOrganizationId(long organizationId) {
        this.organizationId = organizationId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getCp4code() {
        return cp4code;
    }

    public void setCp4code(String cp4code) {
        this.cp4code = cp4code;
    }

    public String getSbu() {
        return sbu;
    }

    public void setSbu(String sbu) {
        this.sbu = sbu;
    }

    public String getCost_center() {
        return cost_center;
    }

    public void setCost_center(String cost_center) {
        this.cost_center = cost_center;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDomainNameKeywords() {
        return domainNameKeywords;
    }

    public void setDomainNameKeywords(String domainNameKeywords) {
        this.domainNameKeywords = domainNameKeywords;
    }

    public String getEnglishName() {
        return englishName;
    }

    public void setEnglishName(String englishName) {
        this.englishName = englishName;
    }
}
