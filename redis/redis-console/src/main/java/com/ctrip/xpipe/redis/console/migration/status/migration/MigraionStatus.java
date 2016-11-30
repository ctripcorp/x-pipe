package com.ctrip.xpipe.redis.console.migration.status.migration;

public enum MigraionStatus {
	Initiated,
	Checking,
	Migrating,
	Publish,
	Success,
	Cancelled,
	PartialSuccess,
	ForcePublish,
	TmpEnd,
	ForceFail
}
