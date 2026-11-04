package com.ctrip.xpipe.redis.meta.server.keeper.manager;


import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Like {@link DeleteKeeperCommand}, but releases the keeper instead of deleting it — for a TFS
 * keeper removed in a non-purge scenario (migration, dead-keeper cleanup), where the shared store
 * must survive this keeper instance. Reuses DeleteKeeperCommand's port-check/retry scaffolding and
 * its {@code isSuccess} (release never produces an error code delete doesn't already handle).
 */
public class ReleaseKeeperCommand extends DeleteKeeperCommand {

	public ReleaseKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
			int timeoutMilli) {
		super(keeperContainerService, keeperTransMeta, scheduled, timeoutMilli);
	}

	public ReleaseKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
			int timeoutMilli, int checkIntervalMilli) {
		super(keeperContainerService, keeperTransMeta, scheduled, timeoutMilli, checkIntervalMilli);
	}

	@Override
	protected void doKeeperContainerOperation() {
		keeperContainerService.releaseKeeper(keeperTransMeta);
	}

}
