package com.ctrip.xpipe.redis.checker.spring;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.annotation.ConfigurationCondition;

public class FiremanServletScanConditionTest {

    @Test
    public void parseConfigurationPhase() {
        Assert.assertEquals(ConfigurationCondition.ConfigurationPhase.PARSE_CONFIGURATION,
                new FiremanServletScanCondition().getConfigurationPhase());
    }

    @Test
    public void scanWhenConsoleAndDbEnabled() {
        Assert.assertTrue(FiremanServletScanCondition.shouldScan(
                ConsoleServerModeCondition.SERVER_MODE.CONSOLE, false));
        Assert.assertTrue(FiremanServletScanCondition.shouldScan(
                ConsoleServerModeCondition.SERVER_MODE.CONSOLE_CHECKER, false));
    }

    @Test
    public void skipWhenCheckerOrNoDb() {
        Assert.assertFalse(FiremanServletScanCondition.shouldScan(
                ConsoleServerModeCondition.SERVER_MODE.CHECKER, false));
        Assert.assertFalse(FiremanServletScanCondition.shouldScan(
                ConsoleServerModeCondition.SERVER_MODE.CONSOLE, true));
        Assert.assertFalse(FiremanServletScanCondition.shouldScan(
                ConsoleServerModeCondition.SERVER_MODE.CONSOLE_CHECKER, true));
        Assert.assertFalse(FiremanServletScanCondition.shouldScan(
                ConsoleServerModeCondition.SERVER_MODE.CHECKER, true));
    }
}
