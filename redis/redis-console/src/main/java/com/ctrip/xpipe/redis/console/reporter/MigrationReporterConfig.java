package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.utils.EncryptUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.ctrip.xpipe.api.config.ConfigProvider.COMMON_CONFIG;

@Configuration
public class MigrationReporterConfig extends AbstractConfigBean {

    private static final String KEY_MIGRATION_PROCESS_REPORT_OPEN = "migration.process.report.open.dc";
    private static final String KEY_MIGRATION_PROCESS_REPORT_URL = "migration.process.report.url";
    private static final String KEY_MIGRATION_BREAK_DOWN_DC= "migration.break.down.dc";
    private static final String KEY_MIGRATION_PROCESS_REPORT_INTERVAL_MILLI = "migration.process.report.interval.milli";

    private static final String KEY_MIGRATION_RESULT_REPORT_URL = "migration.result.report.url";
    private static final String KEY_MIGRATION_RESULT_REPORT_Retry_TIMES = "migration.result.report.retry.times";
    private static final String KEY_MIGRATION_RESULT_REPORT_TOKEN = "migration.result.report.token";
    private static final String KEY_MIGRATION_RESULT_REPORT_OPEN = "migration.result.report.open.dc";
    private static final String KEY_MIGRATION_RESULT_REPORT_INTERVAL_MILLI = "migration.result.report.interval.milli";

    public MigrationReporterConfig() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(COMMON_CONFIG));
    }

    public String getKeyMigrationResultReportToken() {
        String rawToken = getProperty(KEY_MIGRATION_RESULT_REPORT_TOKEN, "");
        try {
            return EncryptUtils.decryptAES_ECB(rawToken, FoundationService.DEFAULT.getAppId());
        } catch (Throwable th) {
            return rawToken;
        }
    }

    public String getKeyMigrationResultReportUrl() {
        return getProperty(KEY_MIGRATION_RESULT_REPORT_URL, "127.0.0.1:8080");
    }

    public boolean isMigrationResultReportOpen() {
        String openDc = getProperty(KEY_MIGRATION_RESULT_REPORT_OPEN, "");
        return StringUtil.trimEquals(openDc, FoundationService.DEFAULT.getDataCenter());
    }

    public int getKeyMigrationResultReportRetryTimes() {
        return getIntProperty(KEY_MIGRATION_RESULT_REPORT_Retry_TIMES, 3);
    }

    public long getMigrationResultReportIntervalMill() {
        return getLongProperty(KEY_MIGRATION_RESULT_REPORT_INTERVAL_MILLI, 10000L);
    }

    public boolean isMigrationProcessReportOpen() {
        String openDc = getProperty(KEY_MIGRATION_PROCESS_REPORT_OPEN, "");
        return StringUtil.trimEquals(openDc, FoundationService.DEFAULT.getDataCenter());
    }

    public String getKeyMigrationProcessReportUrl() {
        return getProperty(KEY_MIGRATION_PROCESS_REPORT_URL, "127.0.0.1:8080");
    }

    public long getMigrationProcessReportIntervalMill() {
        return getLongProperty(KEY_MIGRATION_PROCESS_REPORT_INTERVAL_MILLI, 10000L);
    }

    public Set<String> getBreakDownDc() {
        return getSplitStringSet(getProperty(KEY_MIGRATION_BREAK_DOWN_DC, "jq"));
    }


}
