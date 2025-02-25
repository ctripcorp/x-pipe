package com.ctrip.xpipe.redis.console.keeper.entity;

import java.util.List;
import java.util.Objects;

public class CheckerReportSituation {

    String dc;

    List<Integer> reportedIndex;

    int allIndexCount;

    public CheckerReportSituation() {
    }

    public CheckerReportSituation(String dc, List<Integer> reportedIndex, int allIndexCount) {
        this.dc = dc;
        this.reportedIndex = reportedIndex;
        this.allIndexCount = allIndexCount;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public List<Integer> getReportedIndex() {
        return reportedIndex;
    }

    public void setReportedIndex(List<Integer> reportedIndex) {
        this.reportedIndex = reportedIndex;
    }

    public int getAllIndexCount() {
        return allIndexCount;
    }

    public void setAllIndexCount(int allIndexCount) {
        this.allIndexCount = allIndexCount;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CheckerReportSituation)) return false;
        CheckerReportSituation that = (CheckerReportSituation) o;
        return getAllIndexCount() == that.getAllIndexCount() && Objects.equals(getDc(), that.getDc()) && Objects.equals(getReportedIndex(), that.getReportedIndex());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDc(), getReportedIndex(), getAllIndexCount());
    }

    @Override
    public String toString() {
        return "{" +
                "dc='" + dc + '\'' +
                ", reportedIndex=" + reportedIndex +
                ", allIndexCount=" + allIndexCount +
                '}';
    }
}
