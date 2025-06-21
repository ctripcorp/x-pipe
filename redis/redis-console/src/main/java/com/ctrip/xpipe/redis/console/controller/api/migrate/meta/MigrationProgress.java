package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/5/5
 */
public class MigrationProgress {

    private int total;

    private int success;

    private int fail;

    private int process;

    private int init;

    private long avgMigrationSeconds;

    private Map<String, Integer> activeDcs;

    private Map<MigrationStatus, Long> migrationStatuses;

    public MigrationProgress() {
        this.total = 0;
        this.success = 0;
        this.fail = 0;
        this.process = 0;
        this.init = 0;
        this.avgMigrationSeconds = 0;
        this.activeDcs = new HashMap<>();
        this.migrationStatuses = new HashMap<>();
    }

    public void addMigrationCluster(MigrationClusterTbl migrationClusterTbl) {
        MigrationStatus status = MigrationStatus.valueOf(migrationClusterTbl.getStatus());
        this.total++;

        switch (status.getType()) {
            case MigrationStatus.TYPE_SUCCESS:
                this.success++;
                break;
            case MigrationStatus.TYPE_PROCESSING:
                this.process++;
                break;
            case MigrationStatus.TYPE_FAIL:
                this.fail++;
                break;
            case MigrationStatus.TYPE_INIT:
                this.init++;
                break;
            default:
                this.process++;
                break;
        }

        if (!this.migrationStatuses.containsKey(status)) {
            this.migrationStatuses.put(status, 0L);
        }
        long origin = this.migrationStatuses.get(status);
        this.migrationStatuses.put(status, origin + 1);
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getSuccess() {
        return success;
    }

    public void setSuccess(int success) {
        this.success = success;
    }

    public int getFail() {
        return fail;
    }

    public void setFail(int fail) {
        this.fail = fail;
    }

    public int getProcess() {
        return process;
    }

    public void setProcess(int process) {
        this.process = process;
    }

    public int getInit() {
        return init;
    }

    public void setInit(int init) {
        this.init = init;
    }

    public long getAvgMigrationSeconds() {
        return avgMigrationSeconds;
    }

    public void setAvgMigrationSeconds(long avgMigrationSeconds) {
        this.avgMigrationSeconds = avgMigrationSeconds;
    }

    public Map<String, Integer> getActiveDcs() {
        return activeDcs;
    }

    public void setActiveDcs(Map<String, Integer> activeDcs) {
        this.activeDcs = activeDcs;
    }

    public Map<MigrationStatus, Long> getMigrationStatuses() {
        return migrationStatuses;
    }

    public void setMigrationStatuses(Map<MigrationStatus, Long> migrationStatuses) {
        this.migrationStatuses = migrationStatuses;
    }
}
