package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 28, 2017
 */
public interface ShardMigrationResult {

    ShardMigrationResultStatus getStatus();

    void setStatus(ShardMigrationResultStatus status);

    Map<ShardMigrationStep, Pair<Boolean, String>> getSteps();

    boolean stepTerminated(ShardMigrationStep step);

    boolean stepSuccess(ShardMigrationStep step);

    ShardMigrationStepResult stepResult(ShardMigrationStep step);

    void stepRetry(ShardMigrationStep step);

    void updateStepResult(ShardMigrationStep step, boolean success, String log);

    void setSteps(Map<ShardMigrationStep, Pair<Boolean, String>> steps);

    String encode();

    void setNewMaster(HostPort newMaster);

    HostPort getNewMaster();

    void setPreviousPrimaryDcMessage(MetaServerConsoleService.PreviousPrimaryDcMessage primaryDcMessage);

    MetaServerConsoleService.PreviousPrimaryDcMessage getPreviousPrimaryDcMessage();

}
