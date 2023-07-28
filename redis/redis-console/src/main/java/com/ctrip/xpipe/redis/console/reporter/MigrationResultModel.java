package com.ctrip.xpipe.redis.console.reporter;

import java.util.Objects;

public class MigrationResultModel {

    private String category;

    private String tool;

    private String operator;

    private String subject;

    private String description;

    private String timestamp;

    private String clusterName;

    private String bu;

    private String url;

    private String originMasterDc;

    private String targetMasterDc;

    private String result;

    private long taskId;

    private Object metaData;

    public String getCategory() {
        return category;
    }

    public MigrationResultModel setCategory(String category) {
        this.category = category;
        return this;
    }

    public String getTool() {
        return tool;
    }

    public MigrationResultModel setTool(String tool) {
        this.tool = tool;
        return this;
    }

    public String getOperator() {
        return operator;
    }

    public MigrationResultModel setOperator(String operator) {
        this.operator = operator;
        return this;
    }

    public String getSubject() {
        return subject;
    }

    public MigrationResultModel setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public MigrationResultModel setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public MigrationResultModel setTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public MigrationResultModel setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public String getBu() {
        return bu;
    }

    public MigrationResultModel setBu(String bu) {
        this.bu = bu;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public MigrationResultModel setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getOriginMasterDc() {
        return originMasterDc;
    }

    public MigrationResultModel setOriginMasterDc(String originMasterDc) {
        this.originMasterDc = originMasterDc;
        return this;
    }

    public String getTargetMasterDc() {
        return targetMasterDc;
    }

    public MigrationResultModel setTargetMasterDc(String targetMasterDc) {
        this.targetMasterDc = targetMasterDc;
        return this;
    }

    public String getResult() {
        return result;
    }

    public MigrationResultModel setResult(String result) {
        this.result = result;
        return this;
    }

    public long getTaskId() {
        return taskId;
    }

    public MigrationResultModel setTaskId(long taskId) {
        this.taskId = taskId;
        return this;
    }

    public Object getMetaData() {
        return metaData;
    }

    public MigrationResultModel setMetaData(Object metaData) {
        this.metaData = metaData;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationResultModel that = (MigrationResultModel) o;
        return taskId == that.taskId && Objects.equals(category, that.category) && Objects.equals(tool, that.tool) && Objects.equals(operator, that.operator) && Objects.equals(subject, that.subject) && Objects.equals(description, that.description) && Objects.equals(timestamp, that.timestamp) && Objects.equals(clusterName, that.clusterName) && Objects.equals(bu, that.bu) && Objects.equals(url, that.url) && Objects.equals(originMasterDc, that.originMasterDc) && Objects.equals(targetMasterDc, that.targetMasterDc) && Objects.equals(result, that.result) && Objects.equals(metaData, that.metaData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, tool, operator, subject, description, timestamp, clusterName, bu, url, originMasterDc, targetMasterDc, result, taskId, metaData);
    }

    @Override
    public String toString() {
        return "MigrationResultReportModel{" +
                "category='" + category + '\'' +
                ", tool='" + tool + '\'' +
                ", operator='" + operator + '\'' +
                ", subject='" + subject + '\'' +
                ", description='" + description + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", bu='" + bu + '\'' +
                ", url='" + url + '\'' +
                ", originMasterDc='" + originMasterDc + '\'' +
                ", targetMasterDc='" + targetMasterDc + '\'' +
                ", result='" + result + '\'' +
                ", taskId=" + taskId +
                ", metaData=" + metaData +
                '}';
    }
}
