package com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration;

import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class UnitTestMigrationSystemAvaiableChecker implements MigrationSystemAvailableChecker {

    @Override
    public DefaultMigrationSystemAvailableChecker.MigrationSystemAvailability getResult() {
        return MigrationSystemAvailableChecker
                .MigrationSystemAvailability.createAvailableResponse();
//        MigrationSystemAvailableChecker.MigrationSystemAvailability result =  MigrationSystemAvailableChecker
//                .MigrationSystemAvailability.createUnAvailableResponse();
//        result.addErrorMessage("Database", new NoResponseException("database not response"));
//        return result;
    }
}
