package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.api.observer.Observer;

public interface MigrationCluster extends Observer, MigrationClusterInfoHolder, MigrationClusterAction, MigrationClusterServiceHolder {

}
