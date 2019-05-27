package com.ctrip.xpipe.redis.console.service.migration;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.service.migration.impl.CHECK_MIGRATION_SYSTEM_STEP;

public interface CheckMigrationCommandBuilder {

    Command<RetMessage> checkCommand(CHECK_MIGRATION_SYSTEM_STEP step);
}
