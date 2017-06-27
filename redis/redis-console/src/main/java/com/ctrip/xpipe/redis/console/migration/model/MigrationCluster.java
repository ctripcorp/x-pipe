package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;

import java.util.concurrent.Executor;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public interface MigrationCluster extends Observer, Observable, MigrationClusterInfoHolder, MigrationClusterAction, MigrationClusterServiceHolder {

    Executor getMigrationExecutor();

    MigrationEvent getMigrationEvent();

}
