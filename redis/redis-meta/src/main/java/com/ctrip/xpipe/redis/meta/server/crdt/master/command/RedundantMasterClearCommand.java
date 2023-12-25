package com.ctrip.xpipe.redis.meta.server.crdt.master.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2023/12/22
 */
public class RedundantMasterClearCommand extends AbstractCommand<Set<String>> {

    private Long clusterId;

    private Long shardId;

    private Set<String> dcs;

    private CurrentMetaManager currentMetaManager;

    private static Logger logger = LoggerFactory.getLogger(RedundantMasterClearCommand.class);

    public RedundantMasterClearCommand(Long clusterId, Long shardId, Set<String> dcs, CurrentMetaManager currentMetaManager) {
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.dcs = dcs;
        this.currentMetaManager = currentMetaManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        Set<String> currentDcs = currentMetaManager.getUpstreamPeerDcs(clusterId, shardId);
        Set<String> redundantDcs = new HashSet<>(currentDcs);
        redundantDcs.removeAll(dcs);

        for (String dc: redundantDcs) {
            logger.info("[cluster_{},shard_{}] remove dc {}", clusterId, shardId, dc);
            currentMetaManager.removePeerMaster(dc, clusterId, shardId);
        }

        future().setSuccess(redundantDcs);
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        return String.format("%s[cluster_%d,shard_%d]", getClass().getSimpleName(), clusterId, shardId);
    }
}
