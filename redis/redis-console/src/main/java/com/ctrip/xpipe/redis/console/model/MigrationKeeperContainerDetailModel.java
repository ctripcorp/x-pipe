package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.console.keeper.KeeperContainerOverloadCause;

import java.io.Serializable;
import java.util.List;

public class MigrationKeeperContainerDetailModel implements Serializable {

    private String srcKeeperContainerIp;

    private KeeperContainerOverloadCause overloadCause;

    private List<MigrationKeeperDetailModel> migrationKeeperDetails;


    public String getSrcKeeperContainerIp() {
        return srcKeeperContainerIp;
    }

    public MigrationKeeperContainerDetailModel setSrcKeeperContainerIp(String srcKeeperContainerIp) {
        this.srcKeeperContainerIp = srcKeeperContainerIp;
        return this;
    }

    public KeeperContainerOverloadCause getOverloadCause() {
        return overloadCause;
    }

    public MigrationKeeperContainerDetailModel setOverloadCause(KeeperContainerOverloadCause overloadCause) {
        this.overloadCause = overloadCause;
        return this;
    }

    public List<MigrationKeeperDetailModel> getMigrationKeeperDetails() {
        return migrationKeeperDetails;
    }

    public MigrationKeeperContainerDetailModel setMigrationKeeperDetails(List<MigrationKeeperDetailModel> migrationKeeperDetails) {
        this.migrationKeeperDetails = migrationKeeperDetails;
        return this;
    }
}
