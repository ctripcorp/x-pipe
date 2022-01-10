package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.api.command.CommandFuture;

/**
 * @author lishanglin
 * date 2021/9/23
 */
public interface ReactorMetaServerConsoleService {

    CommandFuture<MetaServerConsoleService.PrimaryDcCheckMessage> changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc);

    CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> makeMasterReadOnly(String clusterId, String shardId, boolean readOnly);

    CommandFuture<MetaServerConsoleService.PrimaryDcChangeMessage> doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, MetaServerConsoleService.PrimaryDcChangeRequest request);

}
