package com.ctrip.xpipe.redis.console.reporter;


import java.util.Objects;

public class MigrationProcessReportModel {

    private String service;

    private String operator;

    private String timestamp;

    private int process;

    private int ObjectCount;

    public MigrationProcessReportModel() {
    }

    public String getService() {

        return service;
    }

    public MigrationProcessReportModel setService(String service) {
        this.service = service;
        return this;
    }

    public String getOperator() {
        return operator;
    }

    public MigrationProcessReportModel setOperator(String operator) {
        this.operator = operator;
        return this;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public MigrationProcessReportModel setTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public int getProcess() {
        return process;
    }

    public MigrationProcessReportModel setProcess(int process) {
        this.process = process;
        return this;
    }

    public int getObjectCount() {
        return ObjectCount;
    }

    public MigrationProcessReportModel setObjectCount(int objectCount) {
        ObjectCount = objectCount;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationProcessReportModel that = (MigrationProcessReportModel) o;
        return process == that.process && ObjectCount == that.ObjectCount && Objects.equals(service, that.service) && Objects.equals(operator, that.operator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, operator, process, ObjectCount);
    }

    @Override
    public String toString() {
        return "MigrationProcessReportModel{" +
                "service='" + service + '\'' +
                ", Operator='" + operator + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", process=" + process +
                ", ObjectCount=" + ObjectCount +
                '}';
    }
}
