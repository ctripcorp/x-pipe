package com.ctrip.xpipe.redis.console.service.logicalbu;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Response of switchBM; counts are per logical BU. {@link #summary} groups failures by layer + code. */
public class KeeperOpResult {

    private int logicalBuCount;
    private int successCount;
    private int failedCount;
    private Set<String> idcs = new TreeSet<>();
    private List<LogicalBuOpResult> results = new ArrayList<>();
    private List<FailureSummary> summary = new ArrayList<>();

    public static KeeperOpResult of(List<LogicalBuOpResult> results) {
        KeeperOpResult result = new KeeperOpResult();
        result.results = results;
        result.logicalBuCount = results.size();

        Map<String, FailureSummary> grouped = new LinkedHashMap<>();
        for (LogicalBuOpResult bu : results) {
            result.idcs.addAll(bu.getIdcs());
            if (bu.getState() == LogicalBuOpResult.State.SUCCESS) {
                result.successCount++;
            } else {
                result.failedCount++;
            }
            for (OpFailure failure : bu.getFailures()) {
                String key = failure.getLayer() + "/" + failure.getCode();
                grouped.computeIfAbsent(key, k -> new FailureSummary(failure.getLayer(), failure.getCode())).count++;
            }
        }
        result.summary = new ArrayList<>(grouped.values());
        return result;
    }

    public static class FailureSummary {
        private final OpLayer layer;
        private final OpErrorCode code;
        private int count;

        FailureSummary(OpLayer layer, OpErrorCode code) {
            this.layer = layer;
            this.code = code;
        }

        public OpLayer getLayer() {
            return layer;
        }

        public OpErrorCode getCode() {
            return code;
        }

        public String getMessage() {
            return code.getMessage();
        }

        public int getCount() {
            return count;
        }
    }

    public int getLogicalBuCount() {
        return logicalBuCount;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public int getFailedCount() {
        return failedCount;
    }

    public Set<String> getIdcs() {
        return idcs;
    }

    public List<LogicalBuOpResult> getResults() {
        return results;
    }

    public List<FailureSummary> getSummary() {
        return summary;
    }
}
