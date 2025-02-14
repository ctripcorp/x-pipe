package com.ctrip.xpipe.redis.console.keeper.entity;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DcCheckerReportMsg {

    CheckerReportSituation checkerReportSituation;

    Map<HostPort, RedisMsg> redisMsg;

    public DcCheckerReportMsg() {
    }

    public DcCheckerReportMsg(Map<HostPort, RedisMsg> redisMsg, CheckerReportSituation checkerReportSituation) {
        this.redisMsg = redisMsg;
        this.checkerReportSituation = checkerReportSituation;
    }

    public DcCheckerReportMsg(Map<HostPort, RedisMsg> redisMsg, String dc, List<Integer> reportedIndex, int allIndexCount) {
        this(redisMsg, new CheckerReportSituation(dc, reportedIndex, allIndexCount));
    }

    public Map<HostPort, RedisMsg> getRedisMsg() {
        return redisMsg;
    }

    public void setRedisMsg(Map<HostPort, RedisMsg> redisMsg) {
        this.redisMsg = redisMsg;
    }

    public CheckerReportSituation getCheckerReportSituation() {
        return checkerReportSituation;
    }

    public void setCheckerReportSituation(CheckerReportSituation checkerReportSituation) {
        this.checkerReportSituation = checkerReportSituation;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DcCheckerReportMsg)) return false;
        DcCheckerReportMsg that = (DcCheckerReportMsg) o;
        return Objects.equals(getRedisMsg(), that.getRedisMsg()) && Objects.equals(getCheckerReportSituation(), that.getCheckerReportSituation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRedisMsg(), getCheckerReportSituation());
    }

    @Override
    public String toString() {
        return "{" +
                "checkerReportSituation=" + checkerReportSituation +
                ", redisMsg=" + redisMsg +
                '}';
    }
}
