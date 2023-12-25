package com.ctrip.xpipe.redis.meta.server.crdt.master;

import com.ctrip.xpipe.redis.meta.server.crdt.master.command.RedundantMasterClearCommand;

import java.util.Set;

public interface MasterChooseCommandFactory {

    RedundantMasterClearCommand buildRedundantMasterClearCommand(Long clusterDbId, Long shardDbId, Set<String> dcs);

    MasterChooseCommand buildPeerMasterChooserCommand(String dcId, Long clusterDbId, Long shardDbId);

    MasterChooseCommand buildCurrentMasterChooserCommand(Long clusterDbId, Long shardDbId);

}
