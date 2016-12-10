package com.ctrip.xpipe.redis.console.migration.model;

import com.ctrip.xpipe.api.observer.Observer;

public interface MigrationShard extends MigratinoShardInfoHolder, MigrationShardAction, Observer{

}
