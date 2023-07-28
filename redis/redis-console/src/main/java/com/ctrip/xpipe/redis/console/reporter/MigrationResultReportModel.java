package com.ctrip.xpipe.redis.console.reporter;

import java.util.List;
import java.util.Objects;

public class MigrationResultReportModel {

    private String access_token;

    private List<MigrationResultModel> request_body;

    public MigrationResultReportModel() {
    }

    public String getAccess_token() {
        return access_token;
    }

    public MigrationResultReportModel setAccess_token(String access_token) {
        this.access_token = access_token;
        return this;
    }

    public List<MigrationResultModel> getRequest_body() {
        return request_body;
    }

    public MigrationResultReportModel setRequest_body(List<MigrationResultModel> request_body) {
        this.request_body = request_body;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationResultReportModel that = (MigrationResultReportModel) o;
        return Objects.equals(access_token, that.access_token) && Objects.equals(request_body, that.request_body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(access_token, request_body);
    }

    @Override
    public String toString() {
        return "MigrationResultReportModel{" +
                "access_token='" + access_token + '\'' +
                ", request_body=" + request_body +
                '}';
    }
}
